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
	"math/rand"
	"org/apache/htrace/common"
	"org/apache/htrace/test"
	"sort"
	"testing"
)

// Test creating and tearing down a datastore.
func TestCreateDatastore(t *testing.T) {
	htraceBld := &MiniHTracedBuilder{Name: "TestCreateDatastore", NumDataDirs: 3}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
}

var SIMPLE_TEST_SPANS []common.Span = []common.Span{
	common.Span{Id: 1,
		SpanData: common.SpanData{
			Begin:       123,
			End:         456,
			Description: "getFileDescriptors",
			TraceId:     999,
			Parents:     []common.SpanId{},
			ProcessId:   "firstd",
		}},
	common.Span{Id: 2,
		SpanData: common.SpanData{
			Begin:       125,
			End:         200,
			Description: "openFd",
			TraceId:     999,
			Parents:     []common.SpanId{1},
			ProcessId:   "secondd",
		}},
	common.Span{Id: 3,
		SpanData: common.SpanData{
			Begin:       200,
			End:         456,
			Description: "passFd",
			TraceId:     999,
			Parents:     []common.SpanId{1},
			ProcessId:   "thirdd",
		}},
}

func createSpans(spans []common.Span, store *dataStore) {
	for idx := range spans {
		store.WriteSpan(&spans[idx])
	}
	// Wait the spans to be created
	for i := 0; i < 3; i++ {
		<-store.WrittenSpans
	}
}

// Test creating a datastore and adding some spans.
func TestDatastoreWriteAndRead(t *testing.T) {
	t.Parallel()
	htraceBld := &MiniHTracedBuilder{Name: "TestDatastoreWriteAndRead",
		WrittenSpans: make(chan *common.Span, 100)}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	createSpans(SIMPLE_TEST_SPANS, ht.Store)
	if ht.Store.GetStatistics().NumSpansWritten < uint64(len(SIMPLE_TEST_SPANS)) {
		t.Fatal()
	}
	span := ht.Store.FindSpan(1)
	if span == nil {
		t.Fatal()
	}
	if span.Id != 1 {
		t.Fatal()
	}
	common.ExpectSpansEqual(t, &SIMPLE_TEST_SPANS[0], span)
	children := ht.Store.FindChildren(1, 1)
	if len(children) != 1 {
		t.Fatalf("expected 1 child, but got %d\n", len(children))
	}
	children = ht.Store.FindChildren(1, 2)
	if len(children) != 2 {
		t.Fatalf("expected 2 children, but got %d\n", len(children))
	}
	sort.Sort(common.Int64Slice(children))
	if children[0] != 2 {
		t.Fatal()
	}
	if children[1] != 3 {
		t.Fatal()
	}
}

func BenchmarkDatastoreWrites(b *testing.B) {
	htraceBld := &MiniHTracedBuilder{Name: "BenchmarkDatastoreWrites",
		WrittenSpans: make(chan *common.Span, b.N)}
	ht, err := htraceBld.Build()
	if err != nil {
		panic(err)
	}
	defer ht.Close()
	rnd := rand.New(rand.NewSource(1))
	allSpans := make([]*common.Span, b.N)
	// Write many random spans.
	for n := 0; n < b.N; n++ {
		span := test.NewRandomSpan(rnd, allSpans[0:n])
		ht.Store.WriteSpan(span)
		allSpans[n] = span
	}
	// Wait for all the spans to be written.
	for n := 0; n < b.N; n++ {
		<-ht.Store.WrittenSpans
	}
	spansWritten := ht.Store.GetStatistics().NumSpansWritten
	if spansWritten < uint64(b.N) {
		b.Fatal("incorrect statistics: expected %d spans to be written, but only got %d",
			b.N, spansWritten)
	}
}
