/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net"
	"net/http"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Set the response headers.
func setResponseHeaders(hdr http.Header) {
	hdr.Set("Content-Type", "application/json")
}

// Write a JSON error response.
func writeError(lg *common.Logger, w http.ResponseWriter, errCode int,
	errStr string) {
	str := strings.Replace(errStr, `"`, `'`, -1)
	lg.Info(str + "\n")
	w.WriteHeader(errCode)
	w.Write([]byte(`{ "error" : "` + str + `"}`))
}

type serverVersionHandler struct {
	lg *common.Logger
}

func (hand *serverVersionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	version := common.ServerVersion{ReleaseVersion: RELEASE_VERSION,
		GitVersion: GIT_VERSION}
	buf, err := json.Marshal(&version)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("error marshalling ServerVersion: %s\n", err.Error()))
		return
	}
	if hand.lg.DebugEnabled() {
		hand.lg.Debugf("Returned ServerVersion %s\n", string(buf))
	}
	w.Write(buf)
}

type serverDebugInfoHandler struct {
	lg *common.Logger
}

func (hand *serverDebugInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	buf := make([]byte, 1<<20)
	common.GetStackTraces(&buf)
	resp := common.ServerDebugInfo{
		StackTraces: string(buf),
		GCStats:     common.GetGCStats(),
	}
	buf, err := json.Marshal(&resp)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("error marshalling ServerDebugInfo: %s\n", err.Error()))
		return
	}
	w.Write(buf)
	hand.lg.Info("Returned ServerDebugInfo\n")
}

type serverStatsHandler struct {
	dataStoreHandler
}

func (hand *serverStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	hand.lg.Debugf("serverStatsHandler\n")
	stats := hand.store.ServerStats()
	buf, err := json.Marshal(&stats)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("error marshalling ServerStats: %s\n", err.Error()))
		return
	}
	hand.lg.Debugf("Returned ServerStats %s\n", string(buf))
	w.Write(buf)
}

type serverConfHandler struct {
	cnf *conf.Config
	lg  *common.Logger
}

func (hand *serverConfHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	hand.lg.Debugf("serverConfHandler\n")
	cnfMap := hand.cnf.Export()
	buf, err := json.Marshal(&cnfMap)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("error marshalling serverConf: %s\n", err.Error()))
		return
	}
	hand.lg.Debugf("Returned server configuration %s\n", string(buf))
	w.Write(buf)
}

type dataStoreHandler struct {
	lg    *common.Logger
	store *dataStore
}

func (hand *dataStoreHandler) parseSid(w http.ResponseWriter,
	str string) (common.SpanId, bool) {
	var id common.SpanId
	err := id.FromString(str)
	if err != nil {
		writeError(hand.lg, w, http.StatusBadRequest,
			fmt.Sprintf("Failed to parse span ID %s: %s", str, err.Error()))
		w.Write([]byte("Error parsing : " + err.Error()))
		return common.INVALID_SPAN_ID, false
	}
	return id, true
}

func (hand *dataStoreHandler) getReqField32(fieldName string, w http.ResponseWriter,
	req *http.Request) (int32, bool) {
	str := req.FormValue(fieldName)
	if str == "" {
		writeError(hand.lg, w, http.StatusBadRequest, fmt.Sprintf("No %s specified.", fieldName))
		return -1, false
	}
	val, err := strconv.ParseUint(str, 16, 32)
	if err != nil {
		writeError(hand.lg, w, http.StatusBadRequest,
			fmt.Sprintf("Error parsing %s: %s.", fieldName, err.Error()))
		return -1, false
	}
	return int32(val), true
}

type findSidHandler struct {
	dataStoreHandler
}

func (hand *findSidHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	req.ParseForm()
	vars := mux.Vars(req)
	stringSid := vars["id"]
	sid, ok := hand.parseSid(w, stringSid)
	if !ok {
		return
	}
	hand.lg.Debugf("findSidHandler(sid=%s)\n", sid.String())
	span := hand.store.FindSpan(sid)
	if span == nil {
		writeError(hand.lg, w, http.StatusNoContent,
			fmt.Sprintf("No such span as %s\n", sid.String()))
		return
	}
	w.Write(span.ToJson())
}

type findChildrenHandler struct {
	dataStoreHandler
}

func (hand *findChildrenHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	req.ParseForm()
	vars := mux.Vars(req)
	stringSid := vars["id"]
	sid, ok := hand.parseSid(w, stringSid)
	if !ok {
		return
	}
	var lim int32
	lim, ok = hand.getReqField32("lim", w, req)
	if !ok {
		return
	}
	hand.lg.Debugf("findChildrenHandler(sid=%s, lim=%d)\n", sid.String(), lim)
	children := hand.store.FindChildren(sid, lim)
	jbytes, err := json.Marshal(children)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("Error marshalling children: %s", err.Error()))
		return
	}
	w.Write(jbytes)
}

type writeSpansHandler struct {
	dataStoreHandler
}

func (hand *writeSpansHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	startTime := time.Now()
	setResponseHeaders(w.Header())
	client, _, serr := net.SplitHostPort(req.RemoteAddr)
	if serr != nil {
		writeError(hand.lg, w, http.StatusBadRequest,
			fmt.Sprintf("Failed to split host and port for %s: %s\n",
				req.RemoteAddr, serr.Error()))
		return
	}
	dec := json.NewDecoder(req.Body)
	var msg common.WriteSpansReq
	err := dec.Decode(&msg)
	if (err != nil) {
		writeError(hand.lg, w, http.StatusBadRequest,
			fmt.Sprintf("Error parsing WriteSpansReq: %s", err.Error()))
		return
	}
	if hand.lg.TraceEnabled() {
		hand.lg.Tracef("%s: read WriteSpans REST message: %s\n",
			req.RemoteAddr, asJson(&msg))
	}
	ing := hand.store.NewSpanIngestor(hand.lg, client, msg.DefaultTrid)
	for spanIdx := 0; spanIdx < msg.NumSpans; spanIdx++ {
		var span *common.Span
		err := dec.Decode(&span)
		if err != nil {
			writeError(hand.lg, w, http.StatusBadRequest,
				fmt.Sprintf("Failed to decode span %d out of %d: ",
					spanIdx, msg.NumSpans, err.Error()))
			return
		}
		ing.IngestSpan(span)
	}
	ing.Close(startTime)
	return
}

type queryHandler struct {
	lg *common.Logger
	dataStoreHandler
}

func (hand *queryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	queryString := req.FormValue("query")
	if queryString == "" {
		writeError(hand.lg, w, http.StatusBadRequest, "No query provided.\n")
		return
	}
	var query common.Query
	reader := bytes.NewBufferString(queryString)
	dec := json.NewDecoder(reader)
	err := dec.Decode(&query)
	if err != nil {
		writeError(hand.lg, w, http.StatusBadRequest,
			fmt.Sprintf("Error parsing query '%s': %s", queryString, err.Error()))
		return
	}
	var results []*common.Span
	results, err = hand.store.HandleQuery(&query)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("Internal error processing query %s: %s",
				query.String(), err.Error()))
		return
	}
	var jbytes []byte
	jbytes, err = json.Marshal(results)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("Error marshalling results: %s", err.Error()))
		return
	}
	w.Write(jbytes)
}

type logErrorHandler struct {
	lg *common.Logger
}

func (hand *logErrorHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	hand.lg.Errorf("Got unknown request %s\n", req.RequestURI)
	writeError(hand.lg, w, http.StatusBadRequest, "Unknown request.")
}

type RestServer struct {
	http.Server
	listener net.Listener
	lg       *common.Logger
}

func CreateRestServer(cnf *conf.Config, store *dataStore,
	listener net.Listener) (*RestServer, error) {
	var err error
	rsv := &RestServer{}
	rsv.lg = common.NewLogger("rest", cnf)

	r := mux.NewRouter().StrictSlash(false)

	r.Handle("/server/info", &serverVersionHandler{lg: rsv.lg}).Methods("GET")
	r.Handle("/server/version", &serverVersionHandler{lg: rsv.lg}).Methods("GET")
	r.Handle("/server/debugInfo", &serverDebugInfoHandler{lg: rsv.lg}).Methods("GET")

	serverStatsH := &serverStatsHandler{dataStoreHandler: dataStoreHandler{
		store: store, lg: rsv.lg}}
	r.Handle("/server/stats", serverStatsH).Methods("GET")

	serverConfH := &serverConfHandler{cnf: cnf, lg: rsv.lg}
	r.Handle("/server/conf", serverConfH).Methods("GET")

	writeSpansH := &writeSpansHandler{dataStoreHandler: dataStoreHandler{
		store: store, lg: rsv.lg}}
	r.Handle("/writeSpans", writeSpansH).Methods("POST")

	queryH := &queryHandler{lg: rsv.lg, dataStoreHandler: dataStoreHandler{store: store}}
	r.Handle("/query", queryH).Methods("GET")

	span := r.PathPrefix("/span").Subrouter()
	findSidH := &findSidHandler{dataStoreHandler: dataStoreHandler{store: store, lg: rsv.lg}}
	span.Handle("/{id}", findSidH).Methods("GET")

	findChildrenH := &findChildrenHandler{dataStoreHandler: dataStoreHandler{store: store,
		lg: rsv.lg}}
	span.Handle("/{id}/children", findChildrenH).Methods("GET")

	// Default Handler. This will serve requests for static requests.
	webdir := os.Getenv("HTRACED_WEB_DIR")
	if webdir == "" {
		webdir, err = filepath.Abs(filepath.Join(filepath.Dir(os.Args[0]), "..", "web"))
		if err != nil {
			return nil, err
		}
	}

	rsv.lg.Infof(`Serving static files from "%s"`+"\n", webdir)
	r.PathPrefix("/").Handler(http.FileServer(http.Dir(webdir))).Methods("GET")

	// Log an error message for unknown non-GET requests.
	r.PathPrefix("/").Handler(&logErrorHandler{lg: rsv.lg})

	rsv.listener = listener
	rsv.Handler = r
	rsv.ErrorLog = rsv.lg.Wrap("[REST] ", common.INFO)
	go rsv.Serve(rsv.listener)
	rsv.lg.Infof("Started REST server on %s\n", rsv.listener.Addr().String())
	return rsv, nil
}

func (rsv *RestServer) Addr() net.Addr {
	return rsv.listener.Addr()
}

func (rsv *RestServer) Close() {
	rsv.listener.Close()
}
