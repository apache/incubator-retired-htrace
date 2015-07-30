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
	"fmt"
	"testing"
	"time"
)

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type SupplierFun func() bool

//
// Wait for a configurable amount of time for a precondition to become true.
//
// Example:
//   WaitFor(time.Minute * 1, time.Millisecond * 1, func() bool {
//      return ht.Store.GetStatistics().NumSpansWritten >= 3
//  })
//
func WaitFor(dur time.Duration, poll time.Duration, fun SupplierFun) {
	if poll == 0 {
		poll = dur / 10
	}
	if poll <= 0 {
		panic("Can't have a polling time less than zero.")
	}
	endTime := time.Now().Add(dur)
	for {
		if fun() {
			return
		}
		if !time.Now().Before(endTime) {
			break
		}
		time.Sleep(poll)
	}
	panic(fmt.Sprintf("Timed out after %s", dur))
}

// Trigger a test failure if two strings are not equal.
func ExpectStrEqual(t *testing.T, expect string, actual string) {
	if expect != actual {
		t.Fatalf("Expected:\n%s\nGot:\n%s\n", expect, actual)
	}
}

// Trigger a test failure if the JSON representation of two spans are not equals.
func ExpectSpansEqual(t *testing.T, spanA *Span, spanB *Span) {
	ExpectStrEqual(t, string(spanA.ToJson()), string(spanB.ToJson()))
}

func TestId(str string) SpanId {
	var spanId SpanId
	err := spanId.FromString(str)
	if err != nil {
		panic(err.Error())
	}
	return spanId
}
