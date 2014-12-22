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
	span := Span{SpanId: 2305843009213693952,
		SpanData: SpanData{
			Start:       123,
			Stop:        456,
			Description: "getFileDescriptors",
			TraceId:     999,
			ParentId:    INVALID_SPAN_ID,
			ProcessId:   331,
		}}
	ExpectStrEqual(t,
		`{"sid":"2305843009213693952","start":"123","stop":"456","desc":"getFileDescriptors","tid":"999","prid":"0","pid":331}`,
		string(span.ToJson()))
}

func TestAnnotatedSpanToJson(t *testing.T) {
	t.Parallel()
	span := Span{SpanId: 1305813009213693952,
		SpanData: SpanData{
			Start:       1234,
			Stop:        4567,
			Description: "getFileDescriptors2",
			TraceId:     999,
			ParentId:    INVALID_SPAN_ID,
			ProcessId:   331,
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
		`{"sid":"1305813009213693952","start":"1234","stop":"4567","desc":"getFileDescriptors2","tid":"999","prid":"0","pid":331,"ta":[{"time":"7777","msg":"contactedServer"},{"time":"8888","msg":"passedFd"}]}`,
		string(span.ToJson()))
}
