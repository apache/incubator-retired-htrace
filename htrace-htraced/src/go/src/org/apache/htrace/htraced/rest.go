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
	"io"
	"net"
	"net/http"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

type serverInfoHandler struct {
	lg *common.Logger
}

func (hand *serverInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setResponseHeaders(w.Header())
	version := common.ServerInfo{ReleaseVersion: RELEASE_VERSION,
		GitVersion: GIT_VERSION}
	buf, err := json.Marshal(&version)
	if err != nil {
		writeError(hand.lg, w, http.StatusInternalServerError,
			fmt.Sprintf("error marshalling ServerInfo: %s\n", err.Error()))
		return
	}
	if hand.lg.DebugEnabled() {
		hand.lg.Debugf("Returned serverInfo %s\n", string(buf))
	}
	w.Write(buf)
}

type dataStoreHandler struct {
	lg    *common.Logger
	store *dataStore
}

func (hand *dataStoreHandler) parseSid(w http.ResponseWriter,
	str string) (common.SpanId, bool) {
	val, err := strconv.ParseUint(str, 16, 64)
	if err != nil {
		writeError(hand.lg, w, http.StatusBadRequest,
			fmt.Sprintf("Failed to parse span ID %s: %s", str, err.Error()))
		w.Write([]byte("Error parsing : " + err.Error()))
		return 0, false
	}
	return common.SpanId(val), true
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
	setResponseHeaders(w.Header())
	dec := json.NewDecoder(req.Body)
	spans := make([]*common.Span, 0, 32)
	defaultPid := req.Header.Get("htrace-pid")
	for {
		var span common.Span
		err := dec.Decode(&span)
		if err != nil {
			if err != io.EOF {
				writeError(hand.lg, w, http.StatusBadRequest,
					fmt.Sprintf("Error parsing spans: %s", err.Error()))
				return
			}
			break
		}
		if span.ProcessId == "" {
			span.ProcessId = defaultPid
		}
		spans = append(spans, &span)
	}
	hand.lg.Debugf("writeSpansHandler: received %d span(s).  defaultPid = %s\n",
		len(spans), defaultPid)
	for spanIdx := range spans {
		if hand.lg.DebugEnabled() {
			hand.lg.Debugf("writing span %s\n", spans[spanIdx].ToJson())
		}
		hand.store.WriteSpan(spans[spanIdx])
	}
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
			fmt.Sprintf("Error parsing query: %s", err.Error()))
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
	listener net.Listener
	lg       *common.Logger
}

func CreateRestServer(cnf *conf.Config, store *dataStore) (*RestServer, error) {
	var err error
	rsv := &RestServer{}
	rsv.listener, err = net.Listen("tcp", cnf.Get(conf.HTRACE_WEB_ADDRESS))
	if err != nil {
		return nil, err
	}
	var success bool
	defer func() {
		if !success {
			rsv.Close()
		}
	}()
	rsv.lg = common.NewLogger("rest", cnf)

	r := mux.NewRouter().StrictSlash(false)

	r.Handle("/server/info", &serverInfoHandler{lg: rsv.lg}).Methods("GET")

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
		webdir, err = filepath.Abs(filepath.Join(filepath.Dir(os.Args[0]), "..", "..", "web"))

		if err != nil {
			return nil, err
		}
	}

	rsv.lg.Infof("Serving static files from %s\n.", webdir)
	r.PathPrefix("/").Handler(http.FileServer(http.Dir(webdir))).Methods("GET")

	// Log an error message for unknown non-GET requests.
	r.PathPrefix("/").Handler(&logErrorHandler{lg: rsv.lg})

	go http.Serve(rsv.listener, r)

	rsv.lg.Infof("Started REST server on %s...\n", rsv.listener.Addr().String())
	success = true
	return rsv, nil
}

func (rsv *RestServer) Addr() net.Addr {
	return rsv.listener.Addr()
}

func (rsv *RestServer) Close() {
	rsv.listener.Close()
}
