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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
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

type SpanId []byte

var INVALID_SPAN_ID SpanId = make([]byte, 16) // all zeroes

func (id SpanId) String() string {
	return fmt.Sprintf("%02x%02x%02x%02x"+
		"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
		id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7], id[8],
		id[9], id[10], id[11], id[12], id[13], id[14], id[15])
}

func (id SpanId) Val() []byte {
	return []byte(id)
}

func (id SpanId) FindProblem() string {
	if id == nil {
		return "The span ID is nil"
	}
	if len(id) != 16 {
		return "The span ID is not exactly 16 bytes."
	}
	if bytes.Equal(id.Val(), INVALID_SPAN_ID.Val()) {
		return "The span ID is all zeros."
	}
	return ""
}

func (id SpanId) ToArray() [16]byte {
	var ret [16]byte
	copy(ret[:], id.Val()[:])
	return ret
}

// Return the next ID in lexicographical order.  For the maximum ID,
// returns the minimum.
func (id SpanId) Next() SpanId {
	next := make([]byte, 16)
	copy(next, id)
	for i := len(next) - 1; i >= 0; i-- {
		if next[i] == 0xff {
			next[i] = 0
		} else {
			next[i] = next[i] + 1
			break
		}
	}
	return next
}

// Return the previous ID in lexicographical order.  For the minimum ID,
// returns the maximum ID.
func (id SpanId) Prev() SpanId {
	prev := make([]byte, 16)
	copy(prev, id)
	for i := len(prev) - 1; i >= 0; i-- {
		if prev[i] == 0x00 {
			prev[i] = 0xff
		} else {
			prev[i] = prev[i] - 1
			break
		}
	}
	return prev
}

func (id SpanId) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.String() + `"`), nil
}

func (id SpanId) Compare(other SpanId) int {
	return bytes.Compare(id.Val(), other.Val())
}

func (id SpanId) Equal(other SpanId) bool {
	return bytes.Equal(id.Val(), other.Val())
}

func (id SpanId) Hash32() uint32 {
	h := fnv.New32a()
	h.Write(id.Val())
	return h.Sum32()
}

type SpanSlice []*Span

func (s SpanSlice) Len() int {
	return len(s)
}

func (s SpanSlice) Less(i, j int) bool {
	return s[i].Id.Compare(s[j].Id) < 0
}

func (s SpanSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SpanIdSlice []SpanId

func (s SpanIdSlice) Len() int {
	return len(s)
}

func (s SpanIdSlice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
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
	i := SpanId(make([]byte, 16))
	n, err := fmt.Sscanf(str, "%02x%02x%02x%02x"+
		"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
		&i[0], &i[1], &i[2], &i[3], &i[4], &i[5], &i[6], &i[7], &i[8],
		&i[9], &i[10], &i[11], &i[12], &i[13], &i[14], &i[15])
	if err != nil {
		return err
	}
	if n != 16 {
		return errors.New("Failed to find 16 hex digits in the SpanId")
	}
	*id = i
	return nil
}

type SpanData struct {
	Begin               int64                `json:"b"`
	End                 int64                `json:"e"`
	Description         string               `json:"d"`
	Parents             []SpanId             `json:"p"`
	Info                TraceInfoMap         `json:"n,omitempty"`
	TracerId            string               `json:"r"`
	TimelineAnnotations []TimelineAnnotation `json:"t,omitempty"`
}

type Span struct {
	Id SpanId `json:"a"`
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
