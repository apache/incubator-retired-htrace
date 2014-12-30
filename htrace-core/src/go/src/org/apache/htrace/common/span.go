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
	"fmt"
	"strconv"
)

//
// Represents a trace span.
//
// Compatibility notes:
// When converting to JSON, we store the 64-bit numbers as hexadecimal strings rather than as
// integers.  This is because JavaScript lacks the ability to handle 64-bit integers.  Numbers above
// about 55 bits will be rounded by Javascript.  Since the Javascript UI is a primary consumer of
// this JSON data, we have to simply pass it as a string.
//

type TraceInfoMap map[string][]byte

type TimelineAnnotation struct {
	Time int64  `json:"time,string"`
	Msg  string `json:"msg"`
}

type SpanId int64

func (id SpanId) String() string {
	return fmt.Sprintf("%08x", id)
}

func (id SpanId) Val() int64 {
	return int64(id)
}

func (id SpanId) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("%016x", uint64(id)) + `"`), nil
}

func (id SpanId) UnMarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(id), 16) + `"`), nil
}

type SpanData struct {
	Begin               int64                `json:"b,string"`
	End                 int64                `json:"e,string"`
	Description         string               `json:"d"`
	TraceId             SpanId               `json:"i"`
	Parents             []SpanId             `json:"p"`
	Info                TraceInfoMap         `json:"n,omitempty"`
	ProcessId           string               `json:"r"`
	TimelineAnnotations []TimelineAnnotation `json:"t,omitempty"`
}

type Span struct {
	Id SpanId `json:"s,string"`
	SpanData
}

func (span *Span) ToJson() []byte {
	jbytes, err := json.Marshal(*span)
	if err != nil {
		panic(err)
	}
	return jbytes
}
