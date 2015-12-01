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

package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	"io"
	"net"
	"net/rpc"
	"org/apache/htrace/common"
)

type hClient struct {
	rpcClient *rpc.Client
}

type HrpcClientCodec struct {
	rwc       io.ReadWriteCloser
	length    uint32
	testHooks *TestHooks
}

func (cdc *HrpcClientCodec) WriteRequest(rr *rpc.Request, msg interface{}) error {
	methodId := common.HrpcMethodNameToId(rr.ServiceMethod)
	if methodId == common.METHOD_ID_NONE {
		return errors.New(fmt.Sprintf("HrpcClientCodec: Unknown method name %s",
			rr.ServiceMethod))
	}
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	w := bytes.NewBuffer(make([]byte, 0, 2048))

	var err error
	enc := codec.NewEncoder(w, mh)
	if methodId == common.METHOD_ID_WRITE_SPANS {
		spans := msg.([]*common.Span)
		req := &common.WriteSpansReq {
			NumSpans: len(spans),
		}
		err = enc.Encode(req)
		if err != nil {
			return errors.New(fmt.Sprintf("HrpcClientCodec: Unable to marshal "+
				"message as msgpack: %s", err.Error()))
		}
		for spanIdx := range(spans) {
			err = enc.Encode(spans[spanIdx])
			if err != nil {
				return errors.New(fmt.Sprintf("HrpcClientCodec: Unable to marshal "+
					"span %d out of %d as msgpack: %s", spanIdx, len(spans), err.Error()))
			}
		}
	} else {
		err = enc.Encode(msg)
		if err != nil {
			return errors.New(fmt.Sprintf("HrpcClientCodec: Unable to marshal "+
				"message as msgpack: %s", err.Error()))
		}
	}
	buf := w.Bytes()
	if len(buf) > common.MAX_HRPC_BODY_LENGTH {
		return errors.New(fmt.Sprintf("HrpcClientCodec: message body is %d "+
			"bytes, but the maximum message size is %d bytes.",
			len(buf), common.MAX_HRPC_BODY_LENGTH))
	}
	hdr := common.HrpcRequestHeader{
		Magic:    common.HRPC_MAGIC,
		MethodId: methodId,
		Seq:      rr.Seq,
		Length:   uint32(len(buf)),
	}
	err = binary.Write(cdc.rwc, binary.LittleEndian, &hdr)
	if err != nil {
		return errors.New(fmt.Sprintf("Error writing header bytes: %s",
			err.Error()))
	}
	if cdc.testHooks != nil && cdc.testHooks.HandleWriteRequestBody != nil {
		cdc.testHooks.HandleWriteRequestBody()
	}
	_, err = cdc.rwc.Write(buf)
	if err != nil {
		return errors.New(fmt.Sprintf("Error writing body bytes: %s",
			err.Error()))
	}
	return nil
}

func (cdc *HrpcClientCodec) ReadResponseHeader(resp *rpc.Response) error {
	hdr := common.HrpcResponseHeader{}
	err := binary.Read(cdc.rwc, binary.LittleEndian, &hdr)
	if err != nil {
		return errors.New(fmt.Sprintf("Error reading response header "+
			"bytes: %s", err.Error()))
	}
	resp.ServiceMethod = common.HrpcMethodIdToMethodName(hdr.MethodId)
	if resp.ServiceMethod == "" {
		return errors.New(fmt.Sprintf("Error reading response header: "+
			"invalid method ID %d.", hdr.MethodId))
	}
	resp.Seq = hdr.Seq
	if hdr.ErrLength > 0 {
		if hdr.ErrLength > common.MAX_HRPC_ERROR_LENGTH {
			return errors.New(fmt.Sprintf("Error reading response header: "+
				"error message was %d bytes long, but "+
				"MAX_HRPC_ERROR_LENGTH is %d.",
				hdr.ErrLength, common.MAX_HRPC_ERROR_LENGTH))
		}
		buf := make([]byte, hdr.ErrLength)
		var nread int
		nread, err = cdc.rwc.Read(buf)
		if uint32(nread) != hdr.ErrLength {
			return errors.New(fmt.Sprintf("Error reading response header: "+
				"failed to read %d bytes of error message.", nread))
		}
		if err != nil {
			return errors.New(fmt.Sprintf("Error reading response header: "+
				"failed to read %d bytes of error message: %s",
				nread, err.Error()))
		}
		resp.Error = string(buf)
	} else {
		resp.Error = ""
	}
	cdc.length = hdr.Length
	return nil
}

func (cdc *HrpcClientCodec) ReadResponseBody(body interface{}) error {
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	dec := codec.NewDecoder(io.LimitReader(cdc.rwc, int64(cdc.length)), mh)
	err := dec.Decode(body)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to read response body: %s",
			err.Error()))
	}
	return nil
}

func (cdc *HrpcClientCodec) Close() error {
	return cdc.rwc.Close()
}

func newHClient(hrpcAddr string, testHooks *TestHooks) (*hClient, error) {
	hcr := hClient{}
	conn, err := net.Dial("tcp", hrpcAddr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error contacting the HRPC server "+
			"at %s: %s", hrpcAddr, err.Error()))
	}
	hcr.rpcClient = rpc.NewClientWithCodec(&HrpcClientCodec{
		rwc:       conn,
		testHooks: testHooks,
	})
	return &hcr, nil
}

func (hcr *hClient) writeSpans(spans []*common.Span) error {
	resp := common.WriteSpansResp{}
	return hcr.rpcClient.Call(common.METHOD_NAME_WRITE_SPANS, spans, &resp)
}

func (hcr *hClient) Close() {
	hcr.rpcClient.Close()
}
