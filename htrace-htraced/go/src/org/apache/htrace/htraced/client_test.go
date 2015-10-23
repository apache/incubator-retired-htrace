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
	htrace "org/apache/htrace/client"
	"org/apache/htrace/common"
	"org/apache/htrace/test"
	"sort"
	"testing"
	"time"
)

func TestClientGetServerInfo(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestClientGetServerInfo",
		DataDirs: make([]string, 2)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf())
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	_, err = hcl.GetServerInfo()
	if err != nil {
		t.Fatalf("failed to call GetServerInfo: %s", err.Error())
	}
}

func createRandomTestSpans(amount int) common.SpanSlice {
	rnd := rand.New(rand.NewSource(2))
	allSpans := make(common.SpanSlice, amount)
	allSpans[0] = test.NewRandomSpan(rnd, allSpans[0:0])
	for i := 1; i < amount; i++ {
		allSpans[i] = test.NewRandomSpan(rnd, allSpans[1:i])
	}
	allSpans[1].SpanData.Parents = []common.SpanId{common.SpanId(allSpans[0].Id)}
	return allSpans
}

func TestClientOperations(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestClientOperations",
		DataDirs: make([]string, 2)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf())
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}

	// Create some random trace spans.
	NUM_TEST_SPANS := 30
	allSpans := createRandomTestSpans(NUM_TEST_SPANS)

	// Write half of the spans to htraced via the client.
	err = hcl.WriteSpans(&common.WriteSpansReq{
		Spans: allSpans[0 : NUM_TEST_SPANS/2],
	})
	if err != nil {
		t.Fatalf("WriteSpans(0:%d) failed: %s\n", NUM_TEST_SPANS/2,
			err.Error())
	}

	// Look up the first half of the spans.  They should be found.
	var span *common.Span
	for i := 0; i < NUM_TEST_SPANS/2; i++ {
		span, err = hcl.FindSpan(allSpans[i].Id)
		if err != nil {
			t.Fatalf("FindSpan(%d) failed: %s\n", i, err.Error())
		}
		common.ExpectSpansEqual(t, allSpans[i], span)
	}

	// Look up the second half of the spans.  They should not be found.
	for i := NUM_TEST_SPANS / 2; i < NUM_TEST_SPANS; i++ {
		span, err = hcl.FindSpan(allSpans[i].Id)
		if err != nil {
			t.Fatalf("FindSpan(%d) failed: %s\n", i, err.Error())
		}
		if span != nil {
			t.Fatalf("Unexpectedly found a span we never write to "+
				"the server: FindSpan(%d) succeeded\n", i)
		}
	}

	// Test FindChildren
	childSpan := allSpans[1]
	parentId := childSpan.Parents[0]
	var children []common.SpanId
	children, err = hcl.FindChildren(parentId, 1)
	if err != nil {
		t.Fatalf("FindChildren(%s) failed: %s\n", parentId, err.Error())
	}
	if len(children) != 1 {
		t.Fatalf("FindChildren(%s) returned an invalid number of "+
			"children: expected %d, got %d\n", parentId, 1, len(children))
	}
	if !children[0].Equal(childSpan.Id) {
		t.Fatalf("FindChildren(%s) returned an invalid child id: expected %s, "+
			" got %s\n", parentId, childSpan.Id, children[0])
	}

	// Test FindChildren on a span that has no children
	childlessSpan := allSpans[NUM_TEST_SPANS/2]
	children, err = hcl.FindChildren(childlessSpan.Id, 10)
	if err != nil {
		t.Fatalf("FindChildren(%d) failed: %s\n", childlessSpan.Id, err.Error())
	}
	if len(children) != 0 {
		t.Fatalf("FindChildren(%d) returned an invalid number of "+
			"children: expected %d, got %d\n", childlessSpan.Id, 0, len(children))
	}

	// Test Query
	var query common.Query
	query = common.Query{Lim: 10}
	spans, err := hcl.Query(&query)
	if err != nil {
		t.Fatalf("Query({lim: %d}) failed: %s\n", 10, err.Error())
	}
	if len(spans) != 10 {
		t.Fatalf("Query({lim: %d}) returned an invalid number of "+
			"children: expected %d, got %d\n", 10, 10, len(spans))
	}
}

func TestDumpAll(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestDumpAll",
		DataDirs: make([]string, 2)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf())
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}

	NUM_TEST_SPANS := 100
	allSpans := createRandomTestSpans(NUM_TEST_SPANS)
	sort.Sort(allSpans)
	err = hcl.WriteSpans(&common.WriteSpansReq{
		Spans: allSpans,
	})
	if err != nil {
		t.Fatalf("WriteSpans failed: %s\n", err.Error())
	}
	out := make(chan *common.Span, 50)
	var dumpErr error
	go func() {
		dumpErr = hcl.DumpAll(3, out)
	}()
	var numSpans int
	nextLogTime := time.Now().Add(time.Millisecond * 5)
	for {
		span, channelOpen := <-out
		if !channelOpen {
			break
		}
		common.ExpectSpansEqual(t, allSpans[numSpans], span)
		numSpans++
		if testing.Verbose() {
			now := time.Now()
			if !now.Before(nextLogTime) {
				nextLogTime = now
				nextLogTime = nextLogTime.Add(time.Millisecond * 5)
				fmt.Printf("read back %d span(s)...\n", numSpans)
			}
		}
	}
	if numSpans != len(allSpans) {
		t.Fatalf("expected to read %d spans... but only read %d\n",
			len(allSpans), numSpans)
	}
	if dumpErr != nil {
		t.Fatalf("got dump error %s\n", dumpErr.Error())
	}
}

const EXAMPLE_CONF_KEY = "example.conf.key"
const EXAMPLE_CONF_VALUE = "foo.bar.baz"

func TestClientGetServerConf(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestClientGetServerConf",
		Cnf: map[string]string{
			EXAMPLE_CONF_KEY: EXAMPLE_CONF_VALUE,
		},
		DataDirs: make([]string, 2)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf())
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	serverCnf, err2 := hcl.GetServerConf()
	if err2 != nil {
		t.Fatalf("failed to call GetServerConf: %s", err2.Error())
	}
	if serverCnf[EXAMPLE_CONF_KEY] != EXAMPLE_CONF_VALUE {
		t.Fatalf("unexpected value for %s: %s",
			EXAMPLE_CONF_KEY, EXAMPLE_CONF_VALUE)
	}
}
