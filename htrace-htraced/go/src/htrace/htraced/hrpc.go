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
	"htrace/common"
	"htrace/conf"
	"io"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

const MAX_HRPC_HANDLERS = 32765

// Handles HRPC calls
type HrpcHandler struct {
	lg    *common.Logger
	store *dataStore
}

// The HRPC server
type HrpcServer struct {
	*rpc.Server
	hand *HrpcHandler

	// The listener we are using to accept new connections.
	listener net.Listener

	// A WaitGroup used to block until the HRPC server has exited.
	exited sync.WaitGroup

	// A channel containing server codecs to use.  This channel is fully
	// buffered.  The number of entries it initially contains determines how
	// many concurrent codecs we will have running at once.
	cdcs chan *HrpcServerCodec

	// Used to shut down
	shutdown chan interface{}

	// The I/O timeout to use when reading requests or sending responses.  This
	// timeout does not apply to the time we spend processing the message.
	ioTimeo time.Duration

	// A count of all I/O errors that we have encountered since the server
	// started.  This counts errors like improperly formatted message frames,
	// but not errors like properly formatted but invalid messages.
	// This count is updated from multiple goroutines via sync/atomic.
	ioErrorCount uint64

	// The test hooks to use, or nil during normal operation.
	testHooks *hrpcTestHooks
}

type hrpcTestHooks struct {
	// A callback we make right after calling Accept() but before reading from
	// the new connection.
	HandleAdmission func()
}

// A codec which encodes HRPC data via JSON.  This structure holds the context
// for a particular client connection.
type HrpcServerCodec struct {
	lg *common.Logger

	// The current connection.
	conn net.Conn

	// The HrpcServer which this connection is part of.
	hsv *HrpcServer

	// The message length we read from the header.
	length uint32

	// The number of messages this connection has handled.
	numHandled int

	// The buffer for reading requests.  These buffers are reused for multiple
	// requests to avoid allocating memory.
	buf []byte

	// Configuration for msgpack decoding
	msgpackHandle codec.MsgpackHandle
}

func asJson(val interface{}) string {
	js, err := json.Marshal(val)
	if err != nil {
		return "encoding error: " + err.Error()
	}
	return string(js)
}

func newIoErrorWarn(cdc *HrpcServerCodec, val string) error {
	return newIoError(cdc, val, common.WARN)
}

func newIoError(cdc *HrpcServerCodec, val string, level common.Level) error {
	if cdc.lg.LevelEnabled(level) {
		cdc.lg.Write(level, cdc.conn.RemoteAddr().String()+": "+val+"\n")
	}
	if level >= common.INFO {
		atomic.AddUint64(&cdc.hsv.ioErrorCount, 1)
	}
	return errors.New(val)
}

func (cdc *HrpcServerCodec) ReadRequestHeader(req *rpc.Request) error {
	hdr := common.HrpcRequestHeader{}
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("%s: Reading HRPC request header.\n", cdc.conn.RemoteAddr())
	}
	cdc.conn.SetDeadline(time.Now().Add(cdc.hsv.ioTimeo))
	err := binary.Read(cdc.conn, binary.LittleEndian, &hdr)
	if err != nil {
		if err == io.EOF && cdc.numHandled > 0 {
			return newIoError(cdc, fmt.Sprintf("Remote closed connection "+
				"after writing %d message(s)", cdc.numHandled), common.DEBUG)
		}
		return newIoError(cdc,
			fmt.Sprintf("Error reading request header: %s", err.Error()), common.WARN)
	}
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("%s: Read HRPC request header %s\n",
			cdc.conn.RemoteAddr(), asJson(&hdr))
	}
	if hdr.Magic != common.HRPC_MAGIC {
		return newIoErrorWarn(cdc, fmt.Sprintf("Invalid request header: expected "+
			"magic number of 0x%04x, but got 0x%04x", common.HRPC_MAGIC, hdr.Magic))
	}
	if hdr.Length > common.MAX_HRPC_BODY_LENGTH {
		return newIoErrorWarn(cdc, fmt.Sprintf("Length prefix was too long.  "+
			"Maximum length is %d, but we got %d.", common.MAX_HRPC_BODY_LENGTH,
			hdr.Length))
	}
	req.ServiceMethod = common.HrpcMethodIdToMethodName(hdr.MethodId)
	if req.ServiceMethod == "" {
		return newIoErrorWarn(cdc, fmt.Sprintf("Unknown MethodID code 0x%04x",
			hdr.MethodId))
	}
	req.Seq = hdr.Seq
	cdc.length = hdr.Length
	return nil
}

func (cdc *HrpcServerCodec) ReadRequestBody(body interface{}) error {
	remoteAddr := cdc.conn.RemoteAddr().String()
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("%s: Reading HRPC %d-byte request body.\n",
			remoteAddr, cdc.length)
	}
	if cap(cdc.buf) < int(cdc.length) {
		var pow uint
		for pow = 0; (1 << pow) < int(cdc.length); pow++ {
		}
		cdc.buf = make([]byte, 0, 1<<pow)
	}
	_, err := io.ReadFull(cdc.conn, cdc.buf[:cdc.length])
	if err != nil {
		return newIoErrorWarn(cdc, fmt.Sprintf("Failed to read %d-byte "+
			"request body: %s", cdc.length, err.Error()))
	}
	var zeroTime time.Time
	cdc.conn.SetDeadline(zeroTime)

	dec := codec.NewDecoderBytes(cdc.buf[:cdc.length], &cdc.msgpackHandle)
	err = dec.Decode(body)
	if cdc.lg.TraceEnabled() {
		cdc.lg.Tracef("%s: read HRPC message: %s\n",
			remoteAddr, asJson(&body))
	}
	req := body.(*common.WriteSpansReq)
	if req == nil {
		return nil
	}
	// We decode WriteSpans requests in a streaming fashion, to avoid overloading the garbage
	// collector with a ton of trace spans all at once.
	startTime := time.Now()
	client, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return newIoErrorWarn(cdc, fmt.Sprintf("Failed to split host and port "+
			"for %s: %s\n", remoteAddr, err.Error()))
	}
	hand := cdc.hsv.hand
	ing := hand.store.NewSpanIngestor(hand.lg, client, req.DefaultTrid)
	for spanIdx := 0; spanIdx < req.NumSpans; spanIdx++ {
		var span *common.Span
		err := dec.Decode(&span)
		if err != nil {
			return newIoErrorWarn(cdc, fmt.Sprintf("Failed to decode span %d "+
				"out of %d: %s\n", spanIdx, req.NumSpans, err.Error()))
		}
		ing.IngestSpan(span)
	}
	ing.Close(startTime)
	return nil
}

var EMPTY []byte = make([]byte, 0)

func (cdc *HrpcServerCodec) WriteResponse(resp *rpc.Response, msg interface{}) error {
	cdc.conn.SetDeadline(time.Now().Add(cdc.hsv.ioTimeo))
	var err error
	buf := EMPTY
	if msg != nil {
		w := bytes.NewBuffer(make([]byte, 0, 128))
		enc := codec.NewEncoder(w, &cdc.msgpackHandle)
		err := enc.Encode(msg)
		if err != nil {
			return newIoErrorWarn(cdc, fmt.Sprintf("Failed to marshal "+
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
		return newIoErrorWarn(cdc, fmt.Sprintf("Failed to write response "+
			"header: %s", err.Error()))
	}
	if hdr.ErrLength > 0 {
		_, err = io.WriteString(writer, resp.Error)
		if err != nil {
			return newIoErrorWarn(cdc, fmt.Sprintf("Failed to write error "+
				"string: %s", err.Error()))
		}
	}
	if hdr.Length > 0 {
		var length int
		length, err = writer.Write(buf)
		if err != nil {
			return newIoErrorWarn(cdc, fmt.Sprintf("Failed to write response "+
				"message: %s", err.Error()))
		}
		if uint32(length) != hdr.Length {
			return newIoErrorWarn(cdc, fmt.Sprintf("Failed to write all of "+
				"response message: %s", err.Error()))
		}
	}
	err = writer.Flush()
	if err != nil {
		return newIoErrorWarn(cdc, fmt.Sprintf("Failed to write the response "+
			"bytes: %s", err.Error()))
	}
	cdc.numHandled++
	return nil
}

func (cdc *HrpcServerCodec) Close() error {
	err := cdc.conn.Close()
	cdc.conn = nil
	cdc.length = 0
	cdc.numHandled = 0
	cdc.hsv.cdcs <- cdc
	return err
}

func (hand *HrpcHandler) WriteSpans(req *common.WriteSpansReq,
	resp *common.WriteSpansResp) (err error) {
	// Nothing to do here; WriteSpans is handled in ReadRequestBody.
	return nil
}

func CreateHrpcServer(cnf *conf.Config, store *dataStore,
	testHooks *hrpcTestHooks) (*HrpcServer, error) {
	lg := common.NewLogger("hrpc", cnf)
	numHandlers := cnf.GetInt(conf.HTRACE_NUM_HRPC_HANDLERS)
	if numHandlers < 1 {
		lg.Warnf("%s must be positive: using 1 handler.\n", conf.HTRACE_NUM_HRPC_HANDLERS)
		numHandlers = 1
	}
	if numHandlers > MAX_HRPC_HANDLERS {
		lg.Warnf("%s cannot be more than %d: using %d handlers\n",
			conf.HTRACE_NUM_HRPC_HANDLERS, MAX_HRPC_HANDLERS, MAX_HRPC_HANDLERS)
		numHandlers = MAX_HRPC_HANDLERS
	}
	hsv := &HrpcServer{
		Server: rpc.NewServer(),
		hand: &HrpcHandler{
			lg:    lg,
			store: store,
		},
		cdcs:     make(chan *HrpcServerCodec, numHandlers),
		shutdown: make(chan interface{}),
		ioTimeo: time.Millisecond *
			time.Duration(cnf.GetInt64(conf.HTRACE_HRPC_IO_TIMEOUT_MS)),
		testHooks: testHooks,
	}
	for i := 0; i < numHandlers; i++ {
		hsv.cdcs <- &HrpcServerCodec{
			lg:  lg,
			hsv: hsv,
			msgpackHandle: codec.MsgpackHandle{
				WriteExt: true,
			},
		}
	}
	var err error
	hsv.listener, err = net.Listen("tcp", cnf.Get(conf.HTRACE_HRPC_ADDRESS))
	if err != nil {
		return nil, err
	}
	hsv.Server.Register(hsv.hand)
	hsv.exited.Add(1)
	go hsv.run()
	lg.Infof("Started HRPC server on %s with %d handler routines. "+
		"ioTimeo=%s.\n", hsv.listener.Addr().String(), numHandlers,
		hsv.ioTimeo.String())
	return hsv, nil
}

func (hsv *HrpcServer) run() {
	lg := hsv.hand.lg
	srvAddr := hsv.listener.Addr().String()
	defer func() {
		lg.Infof("HrpcServer on %s exiting\n", srvAddr)
		hsv.exited.Done()
	}()
	for {
		select {
		case cdc := <-hsv.cdcs:
			conn, err := hsv.listener.Accept()
			if err != nil {
				lg.Errorf("HrpcServer on %s got accept error: %s\n", srvAddr, err.Error())
				hsv.cdcs <- cdc // never blocks; there is always sufficient buffer space
				continue
			}
			if lg.TraceEnabled() {
				lg.Tracef("%s: Accepted HRPC connection.\n", conn.RemoteAddr())
			}
			cdc.conn = conn
			cdc.numHandled = 0
			if hsv.testHooks != nil && hsv.testHooks.HandleAdmission != nil {
				hsv.testHooks.HandleAdmission()
			}
			go hsv.ServeCodec(cdc)
		case <-hsv.shutdown:
			return
		}
	}
}

func (hsv *HrpcServer) Addr() net.Addr {
	return hsv.listener.Addr()
}

func (hsv *HrpcServer) GetNumIoErrors() uint64 {
	return atomic.LoadUint64(&hsv.ioErrorCount)
}

func (hsv *HrpcServer) Close() {
	close(hsv.shutdown)
	hsv.listener.Close()
	hsv.exited.Wait()
}
