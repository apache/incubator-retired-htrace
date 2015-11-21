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
	htrace "org/apache/htrace/client"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"reflect"
	"testing"
	"time"
)

func compareTotals(a, b common.SpanMetricsMap) bool {
	for k, v := range a {
		if !reflect.DeepEqual(v, b[k]) {
			return false
		}
	}
	for k, v := range b {
		if !reflect.DeepEqual(v, a[k]) {
			return false
		}
	}
	return true
}

type Fatalfer interface {
	Fatalf(format string, args ...interface{})
}

func assertNumWrittenEquals(t Fatalfer, msink *MetricsSink,
	expectedNumWritten int) {
	var sstats common.ServerStats
	msink.PopulateServerStats(&sstats)
	if sstats.WrittenSpans != uint64(expectedNumWritten) {
		t.Fatalf("sstats.WrittenSpans = %d, but expected %d\n",
			sstats.WrittenSpans, len(SIMPLE_TEST_SPANS))
	}
	if sstats.HostSpanMetrics["127.0.0.1"] == nil {
		t.Fatalf("no entry for sstats.HostSpanMetrics[127.0.0.1] found.")
	}
	if sstats.HostSpanMetrics["127.0.0.1"].Written !=
		uint64(expectedNumWritten) {
		t.Fatalf("sstats.HostSpanMetrics[127.0.0.1].Written = %d, but "+
			"expected %d\n", sstats.HostSpanMetrics["127.0.0.1"].Written,
			len(SIMPLE_TEST_SPANS))
	}
}

func TestMetricsSinkPerHostEviction(t *testing.T) {
	cnfBld := conf.Builder{
		Values:   conf.TEST_VALUES(),
		Defaults: conf.DEFAULTS,
	}
	cnfBld.Values[conf.HTRACE_METRICS_MAX_ADDR_ENTRIES] = "2"
	cnf, err := cnfBld.Build()
	if err != nil {
		t.Fatalf("failed to create conf: %s", err.Error())
	}
	msink := NewMetricsSink(cnf)
	msink.UpdatePersisted("192.168.0.100", 20, 10)
	msink.UpdatePersisted("192.168.0.101", 20, 10)
	msink.UpdatePersisted("192.168.0.102", 20, 10)
	msink.lock.Lock()
	defer msink.lock.Unlock()
	if len(msink.HostSpanMetrics) != 2 {
		for k, v := range msink.HostSpanMetrics {
			fmt.Printf("WATERMELON: [%s] = [%s]\n", k, v)
		}
		t.Fatalf("Expected len(msink.HostSpanMetrics) to be 2, but got %d\n",
			len(msink.HostSpanMetrics))
	}
}

func TestIngestedSpansMetricsRest(t *testing.T) {
	testIngestedSpansMetricsImpl(t, false)
}

func TestIngestedSpansMetricsPacked(t *testing.T) {
	testIngestedSpansMetricsImpl(t, true)
}

func testIngestedSpansMetricsImpl(t *testing.T, usePacked bool) {
	htraceBld := &MiniHTracedBuilder{Name: "TestIngestedSpansMetrics",
		DataDirs: make([]string, 2),
	}
	ht, err := htraceBld.Build()
	if err != nil {
		t.Fatalf("failed to create datastore: %s", err.Error())
	}
	defer ht.Close()
	var hcl *htrace.Client
	hcl, err = htrace.NewClient(ht.ClientConf(), &htrace.TestHooks{
		HrpcDisabled: !usePacked,
	})
	if err != nil {
		t.Fatalf("failed to create client: %s", err.Error())
	}

	NUM_TEST_SPANS := 12
	allSpans := createRandomTestSpans(NUM_TEST_SPANS)
	err = hcl.WriteSpans(&common.WriteSpansReq{
		Spans: allSpans,
	})
	if err != nil {
		t.Fatalf("WriteSpans failed: %s\n", err.Error())
	}
	for {
		var stats *common.ServerStats
		stats, err = hcl.GetServerStats()
		if err != nil {
			t.Fatalf("GetServerStats failed: %s\n", err.Error())
		}
		if stats.IngestedSpans == uint64(NUM_TEST_SPANS) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func TestCircBuf32(t *testing.T) {
	cbuf := NewCircBufU32(3)
	// We arbitrarily define that empty circular buffers have an average of 0.
	if cbuf.Average() != 0 {
		t.Fatalf("expected empty CircBufU32 to have an average of 0.\n")
	}
	if cbuf.Max() != 0 {
		t.Fatalf("expected empty CircBufU32 to have a max of 0.\n")
	}
	cbuf.Append(2)
	if cbuf.Average() != 2 {
		t.Fatalf("expected one-element CircBufU32 to have an average of 2.\n")
	}
	cbuf.Append(10)
	if cbuf.Average() != 6 {
		t.Fatalf("expected two-element CircBufU32 to have an average of 6.\n")
	}
	cbuf.Append(12)
	if cbuf.Average() != 8 {
		t.Fatalf("expected three-element CircBufU32 to have an average of 8.\n")
	}
	cbuf.Append(14)
	// The 14 overwrites the original 2 element.
	if cbuf.Average() != 12 {
		t.Fatalf("expected three-element CircBufU32 to have an average of 12.\n")
	}
	cbuf.Append(1)
	// The 1 overwrites the original 10 element.
	if cbuf.Average() != 9 {
		t.Fatalf("expected three-element CircBufU32 to have an average of 12.\n")
	}
	if cbuf.Max() != 14 {
		t.Fatalf("expected three-element CircBufU32 to have a max of 14.\n")
	}
}
