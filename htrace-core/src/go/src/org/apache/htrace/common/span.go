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

import (
	"encoding/json"
)

//
// Represents a trace span.
//
// Compatibility notes:
// We use signed numbers here, even in cases where unsigned would make more sense.  This is because
// Java doesn't support unsigned integers, and we'd like to match the representation used by the
// Java client.  For example, if we log a message about a span id in the Java client, it would be
// nice if we could match it up with a log message about the same span id in this server, without
// doing a mental conversion from signed to unsigned.
//
// When converting to JSON, we store the 64-bit numbers as strings rather than as integers.  This is
// because JavaScript lacks the ability to handle 64-bit integers.  Numbers above about 55 bits will
// be rounded by Javascript.  Since the Javascript UI is a primary consumer of this JSON data, we
// have to simply pass it as a string.
//

const INVALID_SPAN_ID = 0

type TraceInfoMap map[string][]byte

type TimelineAnnotation struct {
	Time int64  `json:"time,string"`
	Msg  string `json:"msg"`
}

type SpanIdSlice []int64

type SpanData struct {
	Start               int64                `json:"start,string"`
	Stop                int64                `json:"stop,string"`
	Description         string               `json:"desc"`
	TraceId             int64                `json:"tid,string"`
	ParentId            int64                `json:"prid,string"`
	Info                TraceInfoMap         `json:"info,omitempty"`
	ProcessId           string               `json:"pid"`
	TimelineAnnotations []TimelineAnnotation `json:"ta,omitempty"`
}

type Span struct {
	SpanId int64 `json:"sid,string"`
	SpanData
}

func (span *Span) ToJson() []byte {
	jbytes, err := json.Marshal(*span)
	if err != nil {
		panic(err)
	}
	return jbytes
}

type SpanSlice []Span

func (spans SpanSlice) ToJson() []byte {
	jbytes, err := json.Marshal(spans)
	if err != nil {
		panic(err)
	}
	return jbytes
}
