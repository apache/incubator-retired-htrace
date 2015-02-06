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

package main

import (
	"bytes"
	"org/apache/htrace/common"
	"org/apache/htrace/test"
	"testing"
)

func TestSpansToDot(t *testing.T) {
	TEST_SPANS := common.SpanSlice{
		&common.Span{
			Id: test.SpanId("6af3cc058e5d829d"),
			SpanData: common.SpanData{
				TraceId:     test.SpanId("0e4716fe911244de"),
				Begin:       1424813349020,
				End:         1424813349134,
				Description: "newDFSInputStream",
				ProcessId:   "FsShell",
				Parents:     []common.SpanId{},
				Info: common.TraceInfoMap{
					"path": "/",
				},
			},
		},
		&common.Span{
			Id: test.SpanId("75d16cc5b2c07d8a"),
			SpanData: common.SpanData{
				TraceId:     test.SpanId("0e4716fe911244de"),
				Begin:       1424813349025,
				End:         1424813349133,
				Description: "getBlockLocations",
				ProcessId:   "FsShell",
				Parents:     []common.SpanId{test.SpanId("6af3cc058e5d829d")},
			},
		},
		&common.Span{
			Id: test.SpanId("e2c7273efb280a8c"),
			SpanData: common.SpanData{
				TraceId:     test.SpanId("0e4716fe911244de"),
				Begin:       1424813349027,
				End:         1424813349073,
				Description: "ClientNamenodeProtocol#getBlockLocations",
				ProcessId:   "FsShell",
				Parents:     []common.SpanId{test.SpanId("75d16cc5b2c07d8a")},
			},
		},
	}
	w := bytes.NewBuffer(make([]byte, 0, 2048))
	err := spansToDot(TEST_SPANS, w)
	if err != nil {
		t.Fatalf("spansToDot failed: error %s\n", err.Error())
	}
	EXPECTED_STR := `digraph spans {
  "e2c7273efb280a8c" [label="ClientNamenodeProtocol#getBlockLocations"];
  "6af3cc058e5d829d" [label="newDFSInputStream"];
  "75d16cc5b2c07d8a" [label="getBlockLocations"];
  "6af3cc058e5d829d" -> "75d16cc5b2c07d8a";
  "75d16cc5b2c07d8a" -> "e2c7273efb280a8c";
}`
	if w.String() != EXPECTED_STR {
		t.Fatalf("Expected to get:\n%s\nGot:\n%s\n", EXPECTED_STR, w.String())
	}
}
