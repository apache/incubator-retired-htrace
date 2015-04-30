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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"org/apache/htrace/common"
)

type hClient struct {
	rpcClient *rpc.Client
}

type HrpcClientCodec struct {
	rwc    io.ReadWriteCloser
	length uint32
}

func (cdc *HrpcClientCodec) WriteRequest(req *rpc.Request, msg interface{}) error {
	methodId := common.HrpcMethodNameToId(req.ServiceMethod)
	if methodId == common.METHOD_ID_NONE {
		return errors.New(fmt.Sprintf("HrpcClientCodec: Unknown method name %s",
			req.ServiceMethod))
	}
	buf, err := json.Marshal(msg)
	if err != nil {
		return errors.New(fmt.Sprintf("HrpcClientCodec: Unable to marshal "+
			"message as JSON: %s", err.Error()))
	}
	if len(buf) > common.MAX_HRPC_BODY_LENGTH {
		return errors.New(fmt.Sprintf("HrpcClientCodec: message body is %d "+
			"bytes, but the maximum message size is %d bytes.",
			len(buf), common.MAX_HRPC_BODY_LENGTH))
	}
	hdr := common.HrpcRequestHeader{
		Magic:    common.HRPC_MAGIC,
		MethodId: methodId,
		Seq:      req.Seq,
		Length:   uint32(len(buf)),
	}
	err = binary.Write(cdc.rwc, binary.LittleEndian, &hdr)
	if err != nil {
		return errors.New(fmt.Sprintf("Error writing header bytes: %s",
			err.Error()))
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
	dec := json.NewDecoder(io.LimitReader(cdc.rwc, int64(cdc.length)))
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

func newHClient(hrpcAddr string) (*hClient, error) {
	hcr := hClient{}
	conn, err := net.Dial("tcp", hrpcAddr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error contacting the HRPC server "+
			"at %s: %s", hrpcAddr, err.Error()))
	}
	hcr.rpcClient = rpc.NewClientWithCodec(&HrpcClientCodec{rwc: conn})
	return &hcr, nil
}

func (hcr *hClient) writeSpans(req *common.WriteSpansReq) error {
	resp := common.WriteSpansResp{}
	return hcr.rpcClient.Call(common.METHOD_NAME_WRITE_SPANS, req, &resp)
}

func (hcr *hClient) Close() {
	hcr.rpcClient.Close()
}
