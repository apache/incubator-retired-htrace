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
	"encoding/hex"
	"fmt"
	"github.com/ugorji/go/codec"
	"testing"
)

func TestSpanToJson(t *testing.T) {
	t.Parallel()
	span := Span{Id: TestId("33f25a1a750a471db5bafa59309d7d6f"),
		SpanData: SpanData{
			Begin:       123,
			End:         456,
			Description: "getFileDescriptors",
			Parents:     []SpanId{},
			TracerId:    "testTracerId",
		}}
	ExpectStrEqual(t,
		`{"a":"33f25a1a750a471db5bafa59309d7d6f","b":123,"e":456,"d":"getFileDescriptors","p":[],"r":"testTracerId"}`,
		string(span.ToJson()))
}

func TestAnnotatedSpanToJson(t *testing.T) {
	t.Parallel()
	span := Span{Id: TestId("11eace42e6404b40a7644214cb779a08"),
		SpanData: SpanData{
			Begin:       1234,
			End:         4567,
			Description: "getFileDescriptors2",
			Parents:     []SpanId{},
			TracerId:    "testAnnotatedTracerId",
			TimelineAnnotations: []TimelineAnnotation{
				TimelineAnnotation{
					Time: 7777,
					Msg:  "contactedServer",
				},
				TimelineAnnotation{
					Time: 8888,
					Msg:  "passedFd",
				},
			},
		}}
	ExpectStrEqual(t,
		`{"a":"11eace42e6404b40a7644214cb779a08","b":1234,"e":4567,"d":"getFileDescriptors2","p":[],"r":"testAnnotatedTracerId","t":[{"t":7777,"m":"contactedServer"},{"t":8888,"m":"passedFd"}]}`,
		string(span.ToJson()))
}

func TestSpanNext(t *testing.T) {
	ExpectStrEqual(t, TestId("00000000000000000000000000000001").String(),
		TestId("00000000000000000000000000000000").Next().String())
	ExpectStrEqual(t, TestId("00000000000000000000000000f00000").String(),
		TestId("00000000000000000000000000efffff").Next().String())
	ExpectStrEqual(t, TestId("00000000000000000000000000000000").String(),
		TestId("ffffffffffffffffffffffffffffffff").Next().String())
}

func TestSpanPrev(t *testing.T) {
	ExpectStrEqual(t, TestId("00000000000000000000000000000000").String(),
		TestId("00000000000000000000000000000001").Prev().String())
	ExpectStrEqual(t, TestId("00000000000000000000000000efffff").String(),
		TestId("00000000000000000000000000f00000").Prev().String())
	ExpectStrEqual(t, TestId("ffffffffffffffffffffffffffffffff").String(),
		TestId("00000000000000000000000000000000").Prev().String())
}

func TestSpanMsgPack(t *testing.T) {
	span := Span{Id: TestId("33f25a1a750a471db5bafa59309d7d6f"),
		SpanData: SpanData{
			Begin:       1234,
			End:         5678,
			Description: "getFileDescriptors",
			Parents:     []SpanId{},
			TracerId:   "testTracerId",
		}}
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	w := bytes.NewBuffer(make([]byte, 0, 2048))
	enc := codec.NewEncoder(w, mh)
	err := enc.Encode(span)
	if err != nil {
		t.Fatal("Error encoding span as msgpack: " + err.Error())
	}
	buf := w.Bytes()
	fmt.Printf("span: %s\n", hex.EncodeToString(buf))
	mh = new(codec.MsgpackHandle)
	mh.WriteExt = true
	dec := codec.NewDecoder(bytes.NewReader(buf), mh)
	var span2 Span
	err = dec.Decode(&span2)
	if err != nil {
		t.Fatal("Failed to reverse msgpack encoding for " + span.String())
	}
	ExpectSpansEqual(t, &span, &span2)
}
