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
	"htrace/common"
	"testing"
)

func TestSpansToDot(t *testing.T) {
	TEST_SPANS := common.SpanSlice{
		&common.Span{
			Id: common.TestId("814c8ee0e7984be3a8af00ac64adccb6"),
			SpanData: common.SpanData{
				Begin:       1424813349020,
				End:         1424813349134,
				Description: "newDFSInputStream",
				TracerId:    "FsShell",
				Parents:     []common.SpanId{},
				Info: common.TraceInfoMap{
					"path": "/",
				},
			},
		},
		&common.Span{
			Id: common.TestId("cf2d5de696454548bc055d1e6024054c"),
			SpanData: common.SpanData{
				Begin:       1424813349025,
				End:         1424813349133,
				Description: "getBlockLocations",
				TracerId:    "FsShell",
				Parents:     []common.SpanId{common.TestId("814c8ee0e7984be3a8af00ac64adccb6")},
			},
		},
		&common.Span{
			Id: common.TestId("37623806f9c64483b834b8ea5d6b4827"),
			SpanData: common.SpanData{
				Begin:       1424813349027,
				End:         1424813349073,
				Description: "ClientNamenodeProtocol#getBlockLocations",
				TracerId:    "FsShell",
				Parents:     []common.SpanId{common.TestId("cf2d5de696454548bc055d1e6024054c")},
			},
		},
	}
	w := bytes.NewBuffer(make([]byte, 0, 2048))
	err := spansToDot(TEST_SPANS, w)
	if err != nil {
		t.Fatalf("spansToDot failed: error %s\n", err.Error())
	}
	EXPECTED_STR := `digraph spans {
  "37623806f9c64483b834b8ea5d6b4827" [label="ClientNamenodeProtocol#getBlockLocations"];
  "814c8ee0e7984be3a8af00ac64adccb6" [label="newDFSInputStream"];
  "cf2d5de696454548bc055d1e6024054c" [label="getBlockLocations"];
  "814c8ee0e7984be3a8af00ac64adccb6" -> "cf2d5de696454548bc055d1e6024054c";
  "cf2d5de696454548bc055d1e6024054c" -> "37623806f9c64483b834b8ea5d6b4827";
}
`
	if w.String() != EXPECTED_STR {
		t.Fatalf("Expected to get:\n%s\nGot:\n%s\n", EXPECTED_STR, w.String())
	}
}
