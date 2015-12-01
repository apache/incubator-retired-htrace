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
	"encoding/json"
	"math/rand"
	htrace "org/apache/htrace/client"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"org/apache/htrace/test"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// Test creating and tearing down a datastore.
func TestCreateDatastore(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestCreateDatastore",
		DataDirs: make([]string, 3)}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
}

var SIMPLE_TEST_SPANS []common.Span = []common.Span{
	common.Span{Id: common.TestId("00000000000000000000000000000001"),
		SpanData: common.SpanData{
			Begin:       123,
			End:         456,
			Description: "getFileDescriptors",
			Parents:     []common.SpanId{},
			TracerId:    "firstd",
		}},
	common.Span{Id: common.TestId("00000000000000000000000000000002"),
		SpanData: common.SpanData{
			Begin:       125,
			End:         200,
			Description: "openFd",
			Parents:     []common.SpanId{common.TestId("00000000000000000000000000000001")},
			TracerId:    "secondd",
		}},
	common.Span{Id: common.TestId("00000000000000000000000000000003"),
		SpanData: common.SpanData{
			Begin:       200,
			End:         456,
			Description: "passFd",
			Parents:     []common.SpanId{common.TestId("00000000000000000000000000000001")},
			TracerId:    "thirdd",
		}},
}

func createSpans(spans []common.Span, store *dataStore) {
	ing := store.NewSpanIngestor(store.lg, "127.0.0.1", "")
	for idx := range spans {
		ing.IngestSpan(&spans[idx])
	}
	ing.Close(time.Now())
	store.WrittenSpans.Waits(int64(len(spans)))
}

// Test creating a datastore and adding some spans.
func TestDatastoreWriteAndRead(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestDatastoreWriteAndRead",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)

	span := ht.Store.FindSpan(common.TestId("00000000000000000000000000000001"))
	if span == nil {
		t.Fatal()
	}
	if !span.Id.Equal(common.TestId("00000000000000000000000000000001")) {
		t.Fatal()
	}
	common.ExpectSpansEqual(t, &SIMPLE_TEST_SPANS[0], span)
	children := ht.Store.FindChildren(common.TestId("00000000000000000000000000000001"), 1)
	if len(children) != 1 {
		t.Fatalf("expected 1 child, but got %d\n", len(children))
	}
	children = ht.Store.FindChildren(common.TestId("00000000000000000000000000000001"), 2)
	if len(children) != 2 {
		t.Fatalf("expected 2 children, but got %d\n", len(children))
	}
	sort.Sort(common.SpanIdSlice(children))
	if !children[0].Equal(common.TestId("00000000000000000000000000000002")) {
		t.Fatal()
	}
	if !children[1].Equal(common.TestId("00000000000000000000000000000003")) {
		t.Fatal()
	}
}

func testQuery(t *testing.T, ht *MiniHTraced, query *common.Query,
	expectedSpans []common.Span) {
	spans, err := ht.Store.HandleQuery(query)
	if err != nil {
		t.Fatalf("First query failed: %s\n", err.Error())
	}
	expectedBuf := new(bytes.Buffer)
	dec := json.NewEncoder(expectedBuf)
	err = dec.Encode(expectedSpans)
	if err != nil {
		t.Fatalf("Failed to encode expectedSpans to JSON: %s\n", err.Error())
	}
	spansBuf := new(bytes.Buffer)
	dec = json.NewEncoder(spansBuf)
	err = dec.Encode(spans)
	if err != nil {
		t.Fatalf("Failed to encode result spans to JSON: %s\n", err.Error())
	}
	t.Logf("len(spans) = %d, len(expectedSpans) = %d\n", len(spans),
		len(expectedSpans))
	common.ExpectStrEqual(t, string(expectedBuf.Bytes()), string(spansBuf.Bytes()))
}

// Test queries on the datastore.
func TestSimpleQuery(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestSimpleQuery",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)

	assertNumWrittenEquals(t, ht.Store.msink, len(SIMPLE_TEST_SPANS))

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN_OR_EQUALS,
				Field: common.BEGIN_TIME,
				Val:   "125",
			},
		},
		Lim: 5,
	}, []common.Span{SIMPLE_TEST_SPANS[1], SIMPLE_TEST_SPANS[2]})
}

func TestQueries2(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestQueries2",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)
	assertNumWrittenEquals(t, ht.Store.msink, len(SIMPLE_TEST_SPANS))
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.LESS_THAN_OR_EQUALS,
				Field: common.BEGIN_TIME,
				Val:   "125",
			},
		},
		Lim: 5,
	}, []common.Span{SIMPLE_TEST_SPANS[1], SIMPLE_TEST_SPANS[0]})

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.LESS_THAN_OR_EQUALS,
				Field: common.BEGIN_TIME,
				Val:   "125",
			},
			common.Predicate{
				Op:    common.EQUALS,
				Field: common.DESCRIPTION,
				Val:   "getFileDescriptors",
			},
		},
		Lim: 2,
	}, []common.Span{SIMPLE_TEST_SPANS[0]})

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.EQUALS,
				Field: common.DESCRIPTION,
				Val:   "getFileDescriptors",
			},
		},
		Lim: 2,
	}, []common.Span{SIMPLE_TEST_SPANS[0]})
}

func TestQueries3(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestQueries3",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)
	assertNumWrittenEquals(t, ht.Store.msink, len(SIMPLE_TEST_SPANS))
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.CONTAINS,
				Field: common.DESCRIPTION,
				Val:   "Fd",
			},
			common.Predicate{
				Op:    common.GREATER_THAN_OR_EQUALS,
				Field: common.BEGIN_TIME,
				Val:   "100",
			},
		},
		Lim: 5,
	}, []common.Span{SIMPLE_TEST_SPANS[1], SIMPLE_TEST_SPANS[2]})

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.LESS_THAN_OR_EQUALS,
				Field: common.SPAN_ID,
				Val:   common.TestId("00000000000000000000000000000000").String(),
			},
		},
		Lim: 200,
	}, []common.Span{})

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.LESS_THAN_OR_EQUALS,
				Field: common.SPAN_ID,
				Val:   common.TestId("00000000000000000000000000000002").String(),
			},
		},
		Lim: 200,
	}, []common.Span{SIMPLE_TEST_SPANS[1], SIMPLE_TEST_SPANS[0]})
}

func TestQueries4(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestQueries4",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN,
				Field: common.BEGIN_TIME,
				Val:   "125",
			},
		},
		Lim: 5,
	}, []common.Span{SIMPLE_TEST_SPANS[2]})
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN_OR_EQUALS,
				Field: common.DESCRIPTION,
				Val:   "openFd",
			},
		},
		Lim: 2,
	}, []common.Span{SIMPLE_TEST_SPANS[1], SIMPLE_TEST_SPANS[2]})
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN,
				Field: common.DESCRIPTION,
				Val:   "openFd",
			},
		},
		Lim: 2,
	}, []common.Span{SIMPLE_TEST_SPANS[2]})
}

var TEST_QUERIES5_SPANS []common.Span = []common.Span{
	common.Span{Id: common.TestId("10000000000000000000000000000001"),
		SpanData: common.SpanData{
			Begin:       123,
			End:         456,
			Description: "span1",
			Parents:     []common.SpanId{},
			TracerId:    "myTracer",
		}},
	common.Span{Id: common.TestId("10000000000000000000000000000002"),
		SpanData: common.SpanData{
			Begin:       123,
			End:         200,
			Description: "span2",
			Parents:     []common.SpanId{common.TestId("10000000000000000000000000000001")},
			TracerId:    "myTracer",
		}},
	common.Span{Id: common.TestId("10000000000000000000000000000003"),
		SpanData: common.SpanData{
			Begin:       124,
			End:         457,
			Description: "span3",
			Parents:     []common.SpanId{common.TestId("10000000000000000000000000000001")},
			TracerId:    "myTracer",
		}},
}

func TestQueries5(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestQueries5",
		WrittenSpans: common.NewSemaphore(0),
		DataDirs: make([]string, 1),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(TEST_QUERIES5_SPANS, ht.Store)

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN,
				Field: common.BEGIN_TIME,
				Val:   "123",
			},
		},
		Lim: 5,
	}, []common.Span{TEST_QUERIES5_SPANS[2]})
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN,
				Field: common.END_TIME,
				Val:   "200",
			},
		},
		Lim: 500,
	}, []common.Span{TEST_QUERIES5_SPANS[0], TEST_QUERIES5_SPANS[2]})

	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.LESS_THAN_OR_EQUALS,
				Field: common.END_TIME,
				Val:   "999",
			},
		},
		Lim: 500,
	}, []common.Span{TEST_QUERIES5_SPANS[2],
		TEST_QUERIES5_SPANS[0],
		TEST_QUERIES5_SPANS[1],
	})
}

func BenchmarkDatastoreWrites(b *testing.B) {
	htraceBld := &MiniHTracedBuilder{Name: "BenchmarkDatastoreWrites",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
			conf.HTRACE_LOG_LEVEL:                     "INFO",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		b.Fatalf("Error creating MiniHTraced: %s\n", err.Error())
	}
	ht.Store.lg.Infof("BenchmarkDatastoreWrites: b.N = %d\n", b.N)
	defer func() {
		if r := recover(); r != nil {
			ht.Store.lg.Infof("panic: %s\n", r.(error))
		}
		ht.Close()
	}()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	allSpans := make([]*common.Span, b.N)
	for n := range allSpans {
		allSpans[n] = test.NewRandomSpan(rnd, allSpans[0:n])
	}

	// Reset the timer to avoid including the time required to create new
	// random spans in the benchmark total.
	b.ResetTimer()

	// Write many random spans.
	ing := ht.Store.NewSpanIngestor(ht.Store.lg, "127.0.0.1", "")
	for n := 0; n < b.N; n++ {
		ing.IngestSpan(allSpans[n])
	}
	ing.Close(time.Now())
	// Wait for all the spans to be written.
	ht.Store.WrittenSpans.Waits(int64(b.N))
	assertNumWrittenEquals(b, ht.Store.msink, b.N)
}

func TestReloadDataStore(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestReloadDataStore",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		DataDirs:            make([]string, 2),
		KeepDataDirsOnClose: true,
		WrittenSpans:        common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	dataDirs := make([]string, len(ht.DataDirs))
	copy(dataDirs, ht.DataDirs)
	defer func() {
		if ht != nil {
			ht.Close()
		}
		for i := range dataDirs {
			os.RemoveAll(dataDirs[i])
		}
	}()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}

	// Create some random trace spans.
	NUM_TEST_SPANS := 5
	allSpans := createRandomTestSpans(NUM_TEST_SPANS)
	err = hcl.WriteSpans(allSpans)
	if err != nil {
		t.Fatalf("WriteSpans failed: %s\n", err.Error())
	}
	ht.Store.WrittenSpans.Waits(int64(NUM_TEST_SPANS))

	// Look up the spans we wrote.
	var span *common.Span
	for i := 0; i < NUM_TEST_SPANS; i++ {
		span, err = hcl.FindSpan(allSpans[i].Id)
		if err != nil {
			t.Fatalf("FindSpan(%d) failed: %s\n", i, err.Error())
		}
		common.ExpectSpansEqual(t, allSpans[i], span)
	}

	ht.Close()
	ht = nil

	htraceBld = &MiniHTracedBuilder{Name: "TestReloadDataStore2",
		DataDirs: dataDirs, KeepDataDirsOnClose: true}
	ht, err = htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to re-create datastore: %s", err.Error())
	}
	hcl, err = htrace.NewClient(ht.ClientConf(), nil)
	if err != nil {
		t.Fatalf("failed to re-create client: %s", err.Error())
	}

	// Look up the spans we wrote earlier.
	for i := 0; i < NUM_TEST_SPANS; i++ {
		span, err = hcl.FindSpan(allSpans[i].Id)
		if err != nil {
			t.Fatalf("FindSpan(%d) failed: %s\n", i, err.Error())
		}
		common.ExpectSpansEqual(t, allSpans[i], span)
	}

	// Set an old datastore version number.
	for i := range ht.Store.shards {
		shard := ht.Store.shards[i]
		writeDataStoreVersion(ht.Store, shard.ldb, CURRENT_LAYOUT_VERSION-1)
	}
	ht.Close()
	ht = nil

	htraceBld = &MiniHTracedBuilder{Name: "TestReloadDataStore3",
		DataDirs: dataDirs, KeepDataDirsOnClose: true}
	ht, err = htraceBld.Build()
	if err == nil {
		t.Fatalf("expected the datastore to fail to load after setting an " +
			"incorrect version.\n")
	}
	if !strings.Contains(err.Error(), "Invalid layout version") {
		t.Fatal(`expected the loading error to contain "invalid layout version"` + "\n")
	}

	// It should work with data.store.clear set.
	htraceBld = &MiniHTracedBuilder{Name: "TestReloadDataStore4",
		DataDirs: dataDirs, KeepDataDirsOnClose: true,
		Cnf: map[string]string{conf.HTRACE_DATA_STORE_CLEAR: "true"}}
	ht, err = htraceBld.Build()
	if err != nil {
		t.Fatalf("expected the datastore loading to succeed after setting an "+
			"incorrect version.  But it failed with error %s\n", err.Error())
	}
}

func TestQueriesWithContinuationTokens1(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestQueriesWithContinuationTokens1",
		Cnf: map[string]string{
			conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS: "30000",
		},
		WrittenSpans: common.NewSemaphore(0),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)
	assertNumWrittenEquals(t, ht.Store.msink, len(SIMPLE_TEST_SPANS))
	// Adding a prev value to this query excludes the first result that we
	// would normally get.
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN,
				Field: common.BEGIN_TIME,
				Val:   "120",
			},
		},
		Lim:  5,
		Prev: &SIMPLE_TEST_SPANS[0],
	}, []common.Span{SIMPLE_TEST_SPANS[1], SIMPLE_TEST_SPANS[2]})

	// There is only one result from an EQUALS query on SPAN_ID.
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.EQUALS,
				Field: common.SPAN_ID,
				Val:   common.TestId("00000000000000000000000000000001").String(),
			},
		},
		Lim:  100,
		Prev: &SIMPLE_TEST_SPANS[0],
	}, []common.Span{})

	// When doing a LESS_THAN_OR_EQUALS search, we still don't get back the
	// span we pass as a continuation token. (Primary index edition).
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.LESS_THAN_OR_EQUALS,
				Field: common.SPAN_ID,
				Val:   common.TestId("00000000000000000000000000000002").String(),
			},
		},
		Lim:  100,
		Prev: &SIMPLE_TEST_SPANS[1],
	}, []common.Span{SIMPLE_TEST_SPANS[0]})

	// When doing a GREATER_THAN_OR_EQUALS search, we still don't get back the
	// span we pass as a continuation token. (Secondary index edition).
	testQuery(t, ht, &common.Query{
		Predicates: []common.Predicate{
			common.Predicate{
				Op:    common.GREATER_THAN,
				Field: common.DURATION,
				Val:   "0",
			},
		},
		Lim:  100,
		Prev: &SIMPLE_TEST_SPANS[1],
	}, []common.Span{SIMPLE_TEST_SPANS[2], SIMPLE_TEST_SPANS[0]})
}
