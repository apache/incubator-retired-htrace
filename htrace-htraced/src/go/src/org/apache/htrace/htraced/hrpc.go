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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
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
	rwc    io.ReadWriteCloser
	length uint32
}

func (cdc *HrpcServerCodec) ReadRequestHeader(req *rpc.Request) error {
	hdr := common.HrpcRequestHeader{}
	err := binary.Read(cdc.rwc, binary.BigEndian, &hdr)
	if err != nil {
		return errors.New(fmt.Sprintf("Error reading header bytes: %s", err.Error()))
	}
	if hdr.Magic != common.HRPC_MAGIC {
		return errors.New(fmt.Sprintf("Invalid request header: expected "+
			"magic number of 0x%04x, but got 0x%04x", common.HRPC_MAGIC, hdr.Magic))
	}
	if hdr.Length > common.MAX_HRPC_BODY_LENGTH {
		return errors.New(fmt.Sprintf("Length prefix was too long.  Maximum "+
			"length is %d, but we got %d.", common.MAX_HRPC_BODY_LENGTH, hdr.Length))
	}
	req.ServiceMethod = common.HrpcMethodIdToMethodName(hdr.MethodId)
	if req.ServiceMethod == "" {
		return errors.New(fmt.Sprintf("Unknown MethodID code 0x%04x",
			hdr.MethodId))
	}
	req.Seq = hdr.Seq
	cdc.length = hdr.Length
	return nil
}

func (cdc *HrpcServerCodec) ReadRequestBody(body interface{}) error {
	dec := json.NewDecoder(io.LimitReader(cdc.rwc, int64(cdc.length)))
	err := dec.Decode(body)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to read request body: %s",
			err.Error()))
	}
	return nil
}

var EMPTY []byte = make([]byte, 0)

func (cdc *HrpcServerCodec) WriteResponse(resp *rpc.Response, msg interface{}) error {
	var err error
	buf := EMPTY
	if msg != nil {
		buf, err = json.Marshal(msg)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to marshal response message: %s",
				err.Error()))
		}
	}
	hdr := common.HrpcResponseHeader{}
	hdr.MethodId = common.HrpcMethodNameToId(resp.ServiceMethod)
	hdr.Seq = resp.Seq
	hdr.ErrLength = uint32(len(resp.Error))
	hdr.Length = uint32(len(buf))
	writer := bufio.NewWriterSize(cdc.rwc, 256)
	err = binary.Write(writer, binary.BigEndian, &hdr)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to write response header: %s",
			err.Error()))
	}
	if hdr.ErrLength > 0 {
		_, err = io.WriteString(writer, resp.Error)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to write error string: %s",
				err.Error()))
		}
	}
	if hdr.Length > 0 {
		var length int
		length, err = writer.Write(buf)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to write response "+
				"message: %s", err.Error()))
		}
		if uint32(length) != hdr.Length {
			return errors.New(fmt.Sprintf("Failed to write all of response "+
				"message: %s", err.Error()))
		}
	}
	err = writer.Flush()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to write the response bytes: "+
			"%s", err.Error()))
	}
	return nil
}

func (cdc *HrpcServerCodec) Close() error {
	return cdc.rwc.Close()
}

func (hand *HrpcHandler) WriteSpans(req *common.WriteSpansReq,
	resp *common.WriteSpansResp) (err error) {
	hand.lg.Debugf("hrpc writeSpansHandler: received %d span(s).  "+
		"defaultPid = %s\n", len(req.Spans), req.DefaultPid)
	for i := range req.Spans {
		span := req.Spans[i]
		if span.ProcessId == "" {
			span.ProcessId = req.DefaultPid
		}
		hand.lg.Tracef("writing span %d: %s\n", i, span.ToJson())
		hand.store.WriteSpan(span)
	}
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
		go hsv.ServeCodec(&HrpcServerCodec{
			rwc: conn,
		})
	}
}

func (hsv *HrpcServer) Addr() net.Addr {
	return hsv.listener.Addr()
}

func (hsv *HrpcServer) Close() {
	hsv.listener.Close()
}
