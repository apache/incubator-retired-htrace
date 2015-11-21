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
	"fmt"
	"math/rand"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"org/apache/htrace/test"
	"testing"
	"time"
)

func TestReapingOldSpans(t *testing.T) {
	const NUM_TEST_SPANS = 20
	testSpans := make([]*common.Span, NUM_TEST_SPANS)
	rnd := rand.New(rand.NewSource(2))
	now := common.TimeToUnixMs(time.Now().UTC())
	for i := range testSpans {
		testSpans[i] = test.NewRandomSpan(rnd, testSpans[0:i])
		testSpans[i].Begin = now - int64(NUM_TEST_SPANS-1-i)
		testSpans[i].Description = fmt.Sprintf("Span%02d", i)
	}
	htraceBld := &MiniHTracedBuilder{Name: "TestReapingOldSpans",
		Cnf: map[string]string{
			conf.HTRACE_SPAN_EXPIRY_MS:                fmt.Sprintf("%d", 60*60*1000),
			conf.HTRACE_REAPER_HEARTBEAT_PERIOD_MS:    "1",
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "1",
		},
		WrittenSpans: common.NewSemaphore(0),
		DataDirs:     make([]string, 2),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create mini htraced cluster: %s\n", err.Error())
	}
	ing := ht.Store.NewSpanIngestor(ht.Store.lg, "127.0.0.1", "")
	for spanIdx := range testSpans {
		ing.IngestSpan(testSpans[spanIdx])
	}
	ing.Close(time.Now())
	// Wait the spans to be created
	ht.Store.WrittenSpans.Waits(NUM_TEST_SPANS)
	// Set a reaper date that will remove all the spans except final one.
	ht.Store.rpr.SetReaperDate(now)

	common.WaitFor(5*time.Minute, time.Millisecond, func() bool {
		for i := 0; i < NUM_TEST_SPANS-1; i++ {
			span := ht.Store.FindSpan(testSpans[i].Id)
			if span != nil {
				ht.Store.lg.Debugf("Waiting for %s to be removed...\n",
					testSpans[i].Description)
				return false
			}
		}
		span := ht.Store.FindSpan(testSpans[NUM_TEST_SPANS-1].Id)
		if span == nil {
			ht.Store.lg.Debugf("Did not expect %s to be removed\n",
				testSpans[NUM_TEST_SPANS-1].Description)
			return false
		}
		return true
	})
	defer ht.Close()
}
