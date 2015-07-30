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
	"errors"
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

type TraceInfoMap map[string]string

type TimelineAnnotation struct {
	Time int64  `json:"t"`
	Msg  string `json:"m"`
}

type SpanId uint64

func (id SpanId) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

func (id SpanId) Val() uint64 {
	return uint64(id)
}

func (id SpanId) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("%016x", uint64(id)) + `"`), nil
}

type SpanSlice []*Span

func (s SpanSlice) Len() int {
	return len(s)
}

func (s SpanSlice) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func (s SpanSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SpanIdSlice []SpanId

func (s SpanIdSlice) Len() int {
	return len(s)
}

func (s SpanIdSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s SpanIdSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

const DOUBLE_QUOTE = 0x22

func (id *SpanId) UnmarshalJSON(b []byte) error {
	if b[0] != DOUBLE_QUOTE {
		return errors.New("Expected spanID to start with a string quote.")
	}
	if b[len(b)-1] != DOUBLE_QUOTE {
		return errors.New("Expected spanID to end with a string quote.")
	}
	return id.FromString(string(b[1 : len(b)-1]))
}

func (id *SpanId) FromString(str string) error {
	v, err := strconv.ParseUint(str, 16, 64)
	if err != nil {
		return err
	}
	*id = SpanId(v)
	return nil
}

type SpanData struct {
	Begin               int64                `json:"b"`
	End                 int64                `json:"e"`
	Description         string               `json:"d"`
	TraceId             SpanId               `json:"i"`
	Parents             []SpanId             `json:"p"`
	Info                TraceInfoMap         `json:"n,omitempty"`
	TracerId            string               `json:"r"`
	TimelineAnnotations []TimelineAnnotation `json:"t,omitempty"`
}

type Span struct {
	Id SpanId `json:"s"`
	SpanData
}

func (span *Span) ToJson() []byte {
	jbytes, err := json.Marshal(*span)
	if err != nil {
		panic(err)
	}
	return jbytes
}

func (span *Span) String() string {
	return string(span.ToJson())
}

// Compute the span duration.  We ignore overflow since we never deal with negative times.
func (span *Span) Duration() int64 {
	return span.End - span.Begin
}
