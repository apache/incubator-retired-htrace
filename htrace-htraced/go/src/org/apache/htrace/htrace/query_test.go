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
	"encoding/json"
	"org/apache/htrace/common"
	"reflect"
	"testing"
)

func predsToStr(preds []common.Predicate) string {
	b, err := json.MarshalIndent(preds, "", "  ")
	if err != nil {
		return "JSON marshaling error: " + err.Error()
	}
	return string(b)
}

func checkParseQueryString(t *testing.T, str string, epreds []common.Predicate) {
	preds, err := parseQueryString(str)
	if err != nil {
		t.Fatalf("got unexpected parseQueryString error: %s\n", err.Error())
	}
	if !reflect.DeepEqual(preds, epreds) {
		t.Fatalf("Unexpected result from parseQueryString.  " +
			"Expected: %s, got: %s\n", predsToStr(epreds), predsToStr(preds))
	}
}

func TestParseQueryString(t *testing.T) {
	verbose = testing.Verbose()
	checkParseQueryString(t, "description eq ls", []common.Predicate {
		common.Predicate {
			Op: common.EQUALS,
			Field: common.DESCRIPTION,
			Val: "ls",
		},
	})
	checkParseQueryString(t, "begin gt 123 and end le 456", []common.Predicate {
		common.Predicate {
			Op: common.GREATER_THAN,
			Field: common.BEGIN_TIME,
			Val: "123",
		},
		common.Predicate {
			Op: common.LESS_THAN_OR_EQUALS,
			Field: common.END_TIME,
			Val: "456",
		},
	})
	checkParseQueryString(t, `DESCRIPTION cn "Foo Bar" and ` +
		`BEGIN ge "999" and SPANID eq "4565d8abc4f70ac1216a3f1834c6860b"`,
		[]common.Predicate {
		common.Predicate {
			Op: common.CONTAINS,
			Field: common.DESCRIPTION,
			Val: "Foo Bar",
		},
		common.Predicate {
			Op: common.GREATER_THAN_OR_EQUALS,
			Field: common.BEGIN_TIME,
			Val: "999",
		},
		common.Predicate {
			Op: common.EQUALS,
			Field: common.SPAN_ID,
			Val: "4565d8abc4f70ac1216a3f1834c6860b",
		},
	})
}
