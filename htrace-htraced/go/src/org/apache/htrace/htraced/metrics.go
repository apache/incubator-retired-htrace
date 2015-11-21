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
	"math"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"sync"
	"time"
)

//
// The Metrics Sink for HTraced.
//
// The Metrics sink keeps track of metrics for the htraced daemon.
// It is important to have good metrics so that we can properly manager htraced.  In particular, we
// need to know what rate we are receiving spans at, the main places spans came from.  If spans
// were dropped because of a high sampling rates, we need to know which part of the system dropped
// them so that we can adjust the sampling rate there.
//

const LATENCY_CIRC_BUF_SIZE = 4096

type MetricsSink struct {
	// The metrics sink logger.
	lg *common.Logger

	// The maximum number of entries we shuld allow in the HostSpanMetrics map.
	maxMtx int

	// The total number of spans ingested by the server (counting dropped spans)
	IngestedSpans uint64

	// The total number of spans written to leveldb since the server started.
	WrittenSpans uint64

	// The total number of spans dropped by the server.
	ServerDropped uint64

	// Per-host Span Metrics
	HostSpanMetrics common.SpanMetricsMap

	// The last few writeSpan latencies
	wsLatencyCircBuf *CircBufU32

	// Lock protecting all metrics
	lock sync.Mutex
}

func NewMetricsSink(cnf *conf.Config) *MetricsSink {
	return &MetricsSink {
		lg:               common.NewLogger("metrics", cnf),
		maxMtx:           cnf.GetInt(conf.HTRACE_METRICS_MAX_ADDR_ENTRIES),
		HostSpanMetrics: make(common.SpanMetricsMap),
		wsLatencyCircBuf: NewCircBufU32(LATENCY_CIRC_BUF_SIZE),
	}
}

// Update the total number of spans which were ingested, as well as other
// metrics that get updated during span ingest. 
func (msink *MetricsSink) UpdateIngested(addr string, totalIngested int,
		serverDropped int, wsLatency time.Duration) {
	msink.lock.Lock()
	defer msink.lock.Unlock()
	msink.IngestedSpans += uint64(totalIngested)
	msink.ServerDropped += uint64(serverDropped)
	msink.updateSpanMetrics(addr, 0, serverDropped)
	wsLatencyMs := wsLatency.Nanoseconds() / 1000000
	var wsLatency32 uint32
	if wsLatencyMs > math.MaxUint32 {
		wsLatency32 = math.MaxUint32
	} else {
		wsLatency32 = uint32(wsLatencyMs)
	}
	msink.wsLatencyCircBuf.Append(wsLatency32)
}

// Update the per-host span metrics.  Must be called with the lock held.
func (msink *MetricsSink) updateSpanMetrics(addr string, numWritten int,
		serverDropped int) {
	mtx, found := msink.HostSpanMetrics[addr]
	if !found {
		// Ensure that the per-host span metrics map doesn't grow too large.
		if len(msink.HostSpanMetrics) >= msink.maxMtx {
			// Delete a random entry
			for k := range msink.HostSpanMetrics {
				msink.lg.Warnf("Evicting metrics entry for addr %s "+
					"because there are more than %d addrs.\n", k, msink.maxMtx)
				delete(msink.HostSpanMetrics, k)
				break
			}
		}
		mtx = &common.SpanMetrics { }
		msink.HostSpanMetrics[addr] = mtx
	}
	mtx.Written += uint64(numWritten)
	mtx.ServerDropped += uint64(serverDropped)
}

// Update the total number of spans which were persisted to disk.
func (msink *MetricsSink) UpdatePersisted(addr string, totalWritten int,
		serverDropped int) {
	msink.lock.Lock()
	defer msink.lock.Unlock()
	msink.WrittenSpans += uint64(totalWritten)
	msink.ServerDropped += uint64(serverDropped)
	msink.updateSpanMetrics(addr, totalWritten, serverDropped)
}

// Read the server stats.
func (msink *MetricsSink) PopulateServerStats(stats *common.ServerStats) {
	msink.lock.Lock()
	defer msink.lock.Unlock()
	stats.IngestedSpans = msink.IngestedSpans
	stats.WrittenSpans = msink.WrittenSpans
	stats.ServerDroppedSpans = msink.ServerDropped
	stats.MaxWriteSpansLatencyMs = msink.wsLatencyCircBuf.Max()
	stats.AverageWriteSpansLatencyMs = msink.wsLatencyCircBuf.Average()
	stats.HostSpanMetrics = make(common.SpanMetricsMap)
	for k, v := range(msink.HostSpanMetrics) {
		stats.HostSpanMetrics[k] = &common.SpanMetrics {
			Written: v.Written,
			ServerDropped: v.ServerDropped,
		}
	}
}

// A circular buffer of uint32s which supports appending and taking the
// average, and some other things.
type CircBufU32 struct {
	// The next slot to fill
	slot int

	// The number of slots which are in use.  This number only ever
	// increases until the buffer is full.
	slotsUsed int

	// The buffer
	buf []uint32
}

func NewCircBufU32(size int) *CircBufU32 {
	return &CircBufU32 {
		slotsUsed: -1,
		buf: make([]uint32, size),
	}
}

func (cbuf *CircBufU32) Max() uint32 {
	var max uint32
	for bufIdx := 0; bufIdx < cbuf.slotsUsed; bufIdx++ {
		if cbuf.buf[bufIdx] > max {
			max = cbuf.buf[bufIdx]
		}
	}
	return max
}

func (cbuf *CircBufU32) Average() uint32 {
	var total uint64
	for bufIdx := 0; bufIdx < cbuf.slotsUsed; bufIdx++ {
		total += uint64(cbuf.buf[bufIdx])
	}
	return uint32(total / uint64(cbuf.slotsUsed))
}

func (cbuf *CircBufU32) Append(val uint32) {
	cbuf.buf[cbuf.slot] = val
	cbuf.slot++
	if cbuf.slotsUsed < cbuf.slot {
		cbuf.slotsUsed = cbuf.slot
	}
	if cbuf.slot >= len(cbuf.buf) {
		cbuf.slot = 0
	}
}
