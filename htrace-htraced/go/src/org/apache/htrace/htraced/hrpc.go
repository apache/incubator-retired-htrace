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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	"io"
	"net"
	"net/rpc"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"reflect"
	"time"
)

// Handles HRPC calls
type HrpcHandler struct {
	lg    *common.Logger
	store *dataStore
}

// The HRPC server
type HrpcServer struct {
	*rpc.Server
	hand     *HrpcHandler
	listener net.Listener
}

// Codec which encodes HRPC data via JSON
type HrpcServerCodec struct {
	lg     *common.Logger
	conn   net.Conn
	length uint32
}

func asJson(val interface{}) string {
	js, err := json.Marshal(val)
	if err != nil {
		return "encoding error: " + err.Error()
	}
	return string(js)
}

func createErrAndWarn(lg *common.Logger, val string) error {
	return createErrAndLog(lg, val, common.WARN)
}

func createErrAndLog(lg *common.Logger, val string, level common.Level) error {
	lg.Write(level, val+"\n")
	return errors.New(val)
}

func (cdc *HrpcServerCodec) ReadRequestHeader(req *rpc.Request) error {
	hdr := common.HrpcRequestHeader{}
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("Reading HRPC request header from %s\n", cdc.conn.RemoteAddr())
	}
	err := binary.Read(cdc.conn, binary.LittleEndian, &hdr)
	if err != nil {
		level := common.WARN
		if err == io.EOF {
			level = common.DEBUG
		}
		return createErrAndLog(cdc.lg, fmt.Sprintf("Error reading header bytes: %s",
			err.Error()), level)
	}
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("Read HRPC request header %s from %s\n",
			asJson(&hdr), cdc.conn.RemoteAddr())
	}
	if hdr.Magic != common.HRPC_MAGIC {
		return createErrAndWarn(cdc.lg, fmt.Sprintf("Invalid request header: expected "+
			"magic number of 0x%04x, but got 0x%04x", common.HRPC_MAGIC, hdr.Magic))
	}
	if hdr.Length > common.MAX_HRPC_BODY_LENGTH {
		return createErrAndWarn(cdc.lg, fmt.Sprintf("Length prefix was too long.  "+
			"Maximum length is %d, but we got %d.", common.MAX_HRPC_BODY_LENGTH,
			hdr.Length))
	}
	req.ServiceMethod = common.HrpcMethodIdToMethodName(hdr.MethodId)
	if req.ServiceMethod == "" {
		return createErrAndWarn(cdc.lg, fmt.Sprintf("Unknown MethodID code 0x%04x",
			hdr.MethodId))
	}
	req.Seq = hdr.Seq
	cdc.length = hdr.Length
	return nil
}

func (cdc *HrpcServerCodec) ReadRequestBody(body interface{}) error {
	remoteAddr := cdc.conn.RemoteAddr()
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("Reading HRPC %d-byte request body from %s\n",
			cdc.length, remoteAddr)
	}
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	dec := codec.NewDecoder(io.LimitReader(cdc.conn, int64(cdc.length)), mh)
	err := dec.Decode(body)
	if err != nil {
		return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to read request "+
			"body from %s: %s", remoteAddr, err.Error()))
	}
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("Read body from %s: %s\n",
			remoteAddr, asJson(&body))
	}
	val := reflect.ValueOf(body)
	addr := val.Elem().FieldByName("Addr")
	if addr.IsValid() {
		addr.SetString(remoteAddr.String())
	}
	return nil
}

var EMPTY []byte = make([]byte, 0)

func (cdc *HrpcServerCodec) WriteResponse(resp *rpc.Response, msg interface{}) error {
	var err error
	buf := EMPTY
	if msg != nil {
		mh := new(codec.MsgpackHandle)
		mh.WriteExt = true
		w := bytes.NewBuffer(make([]byte, 0, 128))
		enc := codec.NewEncoder(w, mh)
		err := enc.Encode(msg)
		if err != nil {
			return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to marshal "+
				"response message: %s", err.Error()))
		}
		buf = w.Bytes()
	}
	hdr := common.HrpcResponseHeader{}
	hdr.MethodId = common.HrpcMethodNameToId(resp.ServiceMethod)
	hdr.Seq = resp.Seq
	hdr.ErrLength = uint32(len(resp.Error))
	hdr.Length = uint32(len(buf))
	writer := bufio.NewWriterSize(cdc.conn, 256)
	err = binary.Write(writer, binary.LittleEndian, &hdr)
	if err != nil {
		return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to write response "+
			"header: %s", err.Error()))
	}
	if hdr.ErrLength > 0 {
		_, err = io.WriteString(writer, resp.Error)
		if err != nil {
			return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to write error "+
				"string: %s", err.Error()))
		}
	}
	if hdr.Length > 0 {
		var length int
		length, err = writer.Write(buf)
		if err != nil {
			return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to write response "+
				"message: %s", err.Error()))
		}
		if uint32(length) != hdr.Length {
			return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to write all of "+
				"response message: %s", err.Error()))
		}
	}
	err = writer.Flush()
	if err != nil {
		return createErrAndWarn(cdc.lg, fmt.Sprintf("Failed to write the response "+
			"bytes: %s", err.Error()))
	}
	return nil
}

func (cdc *HrpcServerCodec) Close() error {
	return cdc.conn.Close()
}

func (hand *HrpcHandler) WriteSpans(req *common.WriteSpansReq,
		resp *common.WriteSpansResp) (err error) {
	startTime := time.Now()
	hand.lg.Debugf("hrpc writeSpansHandler: received %d span(s).  "+
		"defaultTrid = %s\n", len(req.Spans), req.DefaultTrid)
	client, _, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to split host and port " +
			"for %s: %s\n", req.Addr, err.Error()))
	}
	for i := range req.Spans {
		span := req.Spans[i]
		spanIdProblem := span.Id.FindProblem()
		if spanIdProblem != "" {
			return errors.New(fmt.Sprintf("Invalid span ID: %s", spanIdProblem))
		}
		if span.TracerId == "" {
			span.TracerId = req.DefaultTrid
		}
		if hand.lg.TraceEnabled() {
			hand.lg.Tracef("writing span %d: %s\n", i, span.ToJson())
		}
		hand.store.WriteSpan(&IncomingSpan{
			Addr: client,
			Span: span,
		})
	}
	endTime := time.Now()
	hand.store.msink.Update(client, req.ClientDropped, len(req.Spans),
			endTime.Sub(startTime))
	return nil
}

func CreateHrpcServer(cnf *conf.Config, store *dataStore) (*HrpcServer, error) {
	lg := common.NewLogger("hrpc", cnf)
	hsv := &HrpcServer{
		Server: rpc.NewServer(),
		hand: &HrpcHandler{
			lg:    lg,
			store: store,
		},
	}
	var err error
	hsv.listener, err = net.Listen("tcp", cnf.Get(conf.HTRACE_HRPC_ADDRESS))
	if err != nil {
		return nil, err
	}
	hsv.Server.Register(hsv.hand)
	go hsv.run()
	lg.Infof("Started HRPC server on %s...\n", hsv.listener.Addr().String())
	return hsv, nil
}

func (hsv *HrpcServer) run() {
	lg := hsv.hand.lg
	for {
		conn, err := hsv.listener.Accept()
		if err != nil {
			lg.Errorf("HRPC Accept error: %s\n", err.Error())
			continue
		}
		if lg.TraceEnabled() {
			lg.Tracef("Accepted HRPC connection from %s\n", conn.RemoteAddr())
		}
		go hsv.ServeCodec(&HrpcServerCodec{
			lg:   lg,
			conn: conn,
		})
	}
}

func (hsv *HrpcServer) Addr() net.Addr {
	return hsv.listener.Addr()
}

func (hsv *HrpcServer) Close() {
	hsv.listener.Close()
}
