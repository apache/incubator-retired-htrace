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
	"github.com/ugorji/go/codec"
	"math"
	"math/rand"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"org/apache/htrace/test"
	"sort"
	"testing"
	"time"
	"sync"
	"sync/atomic"
	htrace "org/apache/htrace/client"
)

func TestClientGetServerVersion(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestClientGetServerVersion",
		DataDirs: make([]string, 2)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	defer hcl.Close()
	_, err = hcl.GetServerVersion()
	if err != nil {
		t.Fatalf("failed to call GetServerVersion: %s", err.Error())
	}
}

func TestClientGetServerDebugInfo(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestClientGetServerDebugInfo",
		DataDirs: make([]string, 2)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	defer hcl.Close()
	debugInfo, err := hcl.GetServerDebugInfo()
	if err != nil {
		t.Fatalf("failed to call GetServerDebugInfo: %s", err.Error())
	}
	if debugInfo.StackTraces == "" {
		t.Fatalf(`debugInfo.StackTraces == ""`)
	}
	if debugInfo.GCStats == "" {
		t.Fatalf(`debugInfo.GCStats == ""`)
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
		DataDirs: make([]string, 2),
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	defer hcl.Close()

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
	ht.Store.WrittenSpans.Waits(int64(NUM_TEST_SPANS/2))

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
		DataDirs: make([]string, 2),
		WrittenSpans: common.NewSemaphore(0),
		Cnf: map[string]string{
			conf.HTRACE_LOG_LEVEL: "INFO",
		},
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	defer hcl.Close()

	NUM_TEST_SPANS := 100
	allSpans := createRandomTestSpans(NUM_TEST_SPANS)
	sort.Sort(allSpans)
	err = hcl.WriteSpans(&common.WriteSpansReq{
		Spans: allSpans,
	})
	if err != nil {
		t.Fatalf("WriteSpans failed: %s\n", err.Error())
	}
	ht.Store.WrittenSpans.Waits(int64(NUM_TEST_SPANS))
	out := make(chan *common.Span, NUM_TEST_SPANS)
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
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	defer hcl.Close()
	serverCnf, err2 := hcl.GetServerConf()
	if err2 != nil {
		t.Fatalf("failed to call GetServerConf: %s", err2.Error())
	}
	if serverCnf[EXAMPLE_CONF_KEY] != EXAMPLE_CONF_VALUE {
		t.Fatalf("unexpected value for %s: %s",
			EXAMPLE_CONF_KEY, EXAMPLE_CONF_VALUE)
	}
}

const TEST_NUM_HRPC_HANDLERS = 2

const TEST_NUM_WRITESPANS = 4

// Tests that HRPC limits the number of simultaneous connections being processed.
func TestHrpcAdmissionsControl(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(TEST_NUM_WRITESPANS)
	var numConcurrentHrpcCalls int32
	testHooks := &hrpcTestHooks {
		HandleAdmission: func() {
			defer wg.Done()
			n := atomic.AddInt32(&numConcurrentHrpcCalls, 1)
			if n > TEST_NUM_HRPC_HANDLERS {
				t.Fatalf("The number of concurrent HRPC calls went above " +
					"%d: it's at %d\n", TEST_NUM_HRPC_HANDLERS, n)
			}
			time.Sleep(1 * time.Millisecond)
			n = atomic.AddInt32(&numConcurrentHrpcCalls, -1)
			if n >= TEST_NUM_HRPC_HANDLERS {
				t.Fatalf("The number of concurrent HRPC calls went above " +
					"%d: it was at %d\n", TEST_NUM_HRPC_HANDLERS, n + 1)
			}
		},
	}
	htraceBld := &MiniHTracedBuilder{Name: "TestHrpcAdmissionsControl",
		DataDirs: make([]string, 2),
		Cnf: map[string]string{
			conf.HTRACE_NUM_HRPC_HANDLERS: fmt.Sprintf("%d", TEST_NUM_HRPC_HANDLERS),
		},
		WrittenSpans: common.NewSemaphore(0),
		HrpcTestHooks: testHooks,
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	// Create some random trace spans.
	allSpans := createRandomTestSpans(TEST_NUM_WRITESPANS)
	for iter := 0; iter < TEST_NUM_WRITESPANS; iter++ {
		go func(i int) {
			err = hcl.WriteSpans(&common.WriteSpansReq{
				Spans: allSpans[i:i+1],
			})
			if err != nil {
				t.Fatalf("WriteSpans failed: %s\n", err.Error())
			}
		}(iter)
	}
	wg.Wait()
	ht.Store.WrittenSpans.Waits(int64(TEST_NUM_WRITESPANS))
}

// Tests that HRPC I/O timeouts work.
func TestHrpcIoTimeout(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestHrpcIoTimeout",
		DataDirs: make([]string, 2),
		Cnf: map[string]string{
			conf.HTRACE_NUM_HRPC_HANDLERS: fmt.Sprintf("%d", TEST_NUM_HRPC_HANDLERS),
			conf.HTRACE_HRPC_IO_TIMEOUT_MS: "1",
		},
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	finishClient := make(chan interface{})
	defer func() {
		// Close the finishClient channel, if it hasn't already been closed. 
		defer func() {recover()}()
		close(finishClient)
	}()
	testHooks := &htrace.TestHooks {
		HandleWriteRequestBody: func() {
			<-finishClient
		},
	}
	hcl, err = htrace.NewClient(ht.ClientConf(), testHooks)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}
	// Create some random trace spans.
	allSpans := createRandomTestSpans(TEST_NUM_WRITESPANS)
	var wg sync.WaitGroup
	wg.Add(TEST_NUM_WRITESPANS)
	for iter := 0; iter < TEST_NUM_WRITESPANS; iter++ {
		go func(i int) {
			defer wg.Done()
			// Ignore the error return because there are internal retries in
			// the client which will make this succeed eventually, usually.
			// Keep in mind that we only block until we have seen
			// TEST_NUM_WRITESPANS I/O errors in the HRPC server-- after that,
			// we let requests through so that the test can exit cleanly.
			hcl.WriteSpans(&common.WriteSpansReq{
				Spans: allSpans[i:i+1],
			})
		}(iter)
	}
	for {
		if ht.Hsv.GetNumIoErrors() >= TEST_NUM_WRITESPANS {
			break
		}
		time.Sleep(1000 * time.Nanosecond)
	}
	close(finishClient)
	wg.Wait()
}

func doWriteSpans(name string, N int, maxSpansPerRpc uint32, b *testing.B) {
	htraceBld := &MiniHTracedBuilder{Name: "doWriteSpans",
		Cnf: map[string]string{
			conf.HTRACE_LOG_LEVEL: "INFO",
		},
		WrittenSpans: common.NewSemaphore(int64(1-N)),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	rnd := rand.New(rand.NewSource(1))
	allSpans := make([]*common.Span, N)
	for n := 0; n < N; n++ {
		allSpans[n] = test.NewRandomSpan(rnd, allSpans[0:n])
	}
	// Determine how many calls to WriteSpans we should make.  Each writeSpans
	// message should be small enough so that it doesn't exceed the max RPC
	// body length limit.  TODO: a production-quality golang client would do
	// this internally rather than needing us to do it here in the unit test.
	bodyLen := (4 * common.MAX_HRPC_BODY_LENGTH) / 5
	reqs := make([]*common.WriteSpansReq, 0, 4)
	curReq := -1
	curReqLen := bodyLen
	var curReqSpans uint32
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	var mbuf [8192]byte
	buf := mbuf[:0]
	enc := codec.NewEncoderBytes(&buf, mh)
	for n := 0; n < N; n++ {
		span := allSpans[n]
		if (curReqSpans >= maxSpansPerRpc) ||
			   (curReqLen >= bodyLen) {
			reqs = append(reqs, &common.WriteSpansReq{})
			curReqLen = 0
			curReq++
			curReqSpans = 0
		}
		buf = mbuf[:0]
		enc.ResetBytes(&buf)
		err := enc.Encode(span)
		if err != nil {
			panic(fmt.Sprintf("Error encoding span %s: %s\n",
				span.String(), err.Error()))
		}
		bufLen := len(buf)
		if bufLen > (bodyLen / 5) {
			panic(fmt.Sprintf("Span too long at %d bytes\n", bufLen))
		}
		curReqLen += bufLen
		reqs[curReq].Spans = append(reqs[curReq].Spans, span)
		curReqSpans++
	}
	ht.Store.lg.Infof("num spans: %d.  num WriteSpansReq calls: %d\n", N, len(reqs))
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create client: %s", err.Error()))
	}
	defer hcl.Close()

	// Reset the timer to avoid including the time required to create new
	// random spans in the benchmark total.
	if b != nil {
		b.ResetTimer()
	}

	// Write many random spans.
	for reqIdx := range(reqs) {
		go func() {
			err = hcl.WriteSpans(reqs[reqIdx])
			if err != nil {
				panic(fmt.Sprintf("failed to send WriteSpans request %d: %s",
					reqIdx, err.Error()))
			}
		}()
	}
	// Wait for all the spans to be written.
	ht.Store.WrittenSpans.Wait()
}

// This is a test of how quickly we can create new spans via WriteSpans RPCs.
// Like BenchmarkDatastoreWrites, it creates b.N spans in the datastore.
// Unlike that benchmark, it sends the spans via RPC.
// Suggested flags for running this:
// -tags unsafe -cpu 16 -benchtime=1m
func BenchmarkWriteSpans(b *testing.B) {
	doWriteSpans("BenchmarkWriteSpans", b.N, math.MaxUint32, b)
}

func TestWriteSpansRpcs(t *testing.T) {
	doWriteSpans("TestWriteSpansRpcs", 3000, 1000, nil)
}
