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
	"testing"
)

func TestSpanToJson(t *testing.T) {
	t.Parallel()
	span := Span{Id: 2305843009213693952,
		SpanData: SpanData{
			Begin:       123,
			End:         456,
			Description: "getFileDescriptors",
			TraceId:     999,
			Parents:     []SpanId{},
			ProcessId:   "testProcessId",
		}}
	ExpectStrEqual(t,
		`{"s":"2000000000000000","b":123,"e":456,"d":"getFileDescriptors","i":"00000000000003e7","p":[],"r":"testProcessId"}`,
		string(span.ToJson()))
}

func TestAnnotatedSpanToJson(t *testing.T) {
	t.Parallel()
	span := Span{Id: 1305813009213693952,
		SpanData: SpanData{
			Begin:       1234,
			End:         4567,
			Description: "getFileDescriptors2",
			TraceId:     999,
			Parents:     []SpanId{},
			ProcessId:   "testAnnotatedProcessId",
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
		`{"s":"121f2e036d442000","b":1234,"e":4567,"d":"getFileDescriptors2","i":"00000000000003e7","p":[],"r":"testAnnotatedProcessId","t":[{"t":7777,"m":"contactedServer"},{"t":8888,"m":"passedFd"}]}`,
		string(span.ToJson()))
}
