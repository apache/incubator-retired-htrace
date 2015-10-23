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

package common

// The 4-byte magic number which is sent first in the HRPC header
const HRPC_MAGIC = 0x43525448

// Method ID codes.  Do not reorder these.
const (
	METHOD_ID_NONE        = 0
	METHOD_ID_WRITE_SPANS = iota
)

const METHOD_NAME_WRITE_SPANS = "HrpcHandler.WriteSpans"

// Maximum length of the error message passed in an HRPC response
const MAX_HRPC_ERROR_LENGTH = 4 * 1024 * 1024

// Maximum length of HRPC message body
const MAX_HRPC_BODY_LENGTH = 64 * 1024 * 1024

// A request to write spans to htraced.
type WriteSpansReq struct {
	Addr          string // This gets filled in by the RPC layer.
	DefaultTrid   string `json:",omitempty"`
	Spans         []*Span
	ClientDropped uint64 `json:",omitempty"`
}

// Info returned by /server/info
type ServerInfo struct {
	// The server release version.
	ReleaseVersion string

	// The git hash that this software was built with.
	GitVersion string
}

// A response to a WriteSpansReq
type WriteSpansResp struct {
}

// The header which is sent over the wire for HRPC
type HrpcRequestHeader struct {
	Magic    uint32
	MethodId uint32
	Seq      uint64
	Length   uint32
}

// The response which is sent over the wire for HRPC
type HrpcResponseHeader struct {
	Seq       uint64
	MethodId  uint32
	ErrLength uint32
	Length    uint32
}

func HrpcMethodIdToMethodName(id uint32) string {
	switch id {
	case METHOD_ID_WRITE_SPANS:
		return METHOD_NAME_WRITE_SPANS
	default:
		return ""
	}
}

func HrpcMethodNameToId(name string) uint32 {
	switch name {
	case METHOD_NAME_WRITE_SPANS:
		return METHOD_ID_WRITE_SPANS
	default:
		return METHOD_ID_NONE
	}
}

type SpanMetrics struct {
	// The total number of spans written to HTraced.
	Written uint64

	// The total number of spans dropped by the server.
	ServerDropped uint64

	// The total number of spans dropped by the client.  Note that this number
	// is tracked on the client itself and doesn't get updated if the client
	// can't contact the server.
	ClientDropped uint64
}

// A map from network address strings to SpanMetrics structures.
type SpanMetricsMap map[string]*SpanMetrics

// Info returned by /server/stats
type ServerStats struct {
	// Statistics for each shard (directory)
	Dirs []StorageDirectoryStats

	// Per-host Span Metrics
	HostSpanMetrics SpanMetricsMap
}

type StorageDirectoryStats struct {
	Path string

	// The approximate number of spans present in this shard.  This may be an
	// underestimate.
	ApproxNumSpans uint64

	// leveldb.stats information
	LevelDbStats string
}
