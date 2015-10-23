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
	"org/apache/htrace/conf"
	"sync"
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

type ServerSpanMetrics struct {
	// The total number of spans written to HTraced.
	Written uint64

	// The total number of spans dropped by the server.
	ServerDropped uint64
}

func (spm *ServerSpanMetrics) Clone() *ServerSpanMetrics {
	return &ServerSpanMetrics{
		Written:       spm.Written,
		ServerDropped: spm.ServerDropped,
	}
}

func (spm *ServerSpanMetrics) String() string {
	jbytes, err := json.Marshal(*spm)
	if err != nil {
		panic(err)
	}
	return string(jbytes)
}

func (spm *ServerSpanMetrics) Add(ospm *ServerSpanMetrics) {
	spm.Written += ospm.Written
	spm.ServerDropped += ospm.ServerDropped
}

func (spm *ServerSpanMetrics) Clear() {
	spm.Written = 0
	spm.ServerDropped = 0
}

// A map from network address strings to ServerSpanMetrics structures.
type ServerSpanMetricsMap map[string]*ServerSpanMetrics

func (smtxMap ServerSpanMetricsMap) IncrementDropped(addr string, maxMtx int,
	lg *common.Logger) {
	mtx := smtxMap[addr]
	if mtx == nil {
		mtx = &ServerSpanMetrics{}
		smtxMap[addr] = mtx
	}
	mtx.ServerDropped++
	smtxMap.Prune(maxMtx, lg)
}

func (smtxMap ServerSpanMetricsMap) IncrementWritten(addr string, maxMtx int,
	lg *common.Logger) {
	mtx := smtxMap[addr]
	if mtx == nil {
		mtx = &ServerSpanMetrics{}
		smtxMap[addr] = mtx
	}
	mtx.Written++
	smtxMap.Prune(maxMtx, lg)
}

func (smtxMap ServerSpanMetricsMap) Prune(maxMtx int, lg *common.Logger) {
	if len(smtxMap) >= maxMtx {
		// Delete a random entry
		for k := range smtxMap {
			lg.Warnf("Evicting metrics entry for addr %s "+
				"because there are more than %d addrs.\n", k, maxMtx)
			delete(smtxMap, k)
			return
		}
	}
}

type AccessReq struct {
	mtxMap common.SpanMetricsMap
	done   chan interface{}
}

type MetricsSink struct {
	// The total span metrics.
	smtxMap ServerSpanMetricsMap

	// A channel of incoming shard metrics.
	// When this is shut down, the MetricsSink will exit.
	updateReqs chan ServerSpanMetricsMap

	// A channel of incoming requests for shard metrics.
	accessReqs chan *AccessReq

	// This will be closed when the MetricsSink has exited.
	exited chan interface{}

	// The logger used by this MetricsSink.
	lg *common.Logger

	// The maximum number of metrics totals we will maintain.
	maxMtx int

	// The number of spans which each client has self-reported that it has
	// dropped.
	clientDroppedMap map[string]uint64

	// Lock protecting clientDropped
	clientDroppedLock sync.Mutex
}

func NewMetricsSink(cnf *conf.Config) *MetricsSink {
	mcl := MetricsSink{
		smtxMap:          make(ServerSpanMetricsMap),
		updateReqs:       make(chan ServerSpanMetricsMap, 128),
		accessReqs:       make(chan *AccessReq),
		exited:           make(chan interface{}),
		lg:               common.NewLogger("metrics", cnf),
		maxMtx:           cnf.GetInt(conf.HTRACE_METRICS_MAX_ADDR_ENTRIES),
		clientDroppedMap: make(map[string]uint64),
	}
	go mcl.run()
	return &mcl
}

func (msink *MetricsSink) run() {
	lg := msink.lg
	defer func() {
		lg.Info("MetricsSink: stopping service goroutine.\n")
		close(msink.exited)
	}()
	lg.Tracef("MetricsSink: starting.\n")
	for {
		select {
		case updateReq, open := <-msink.updateReqs:
			if !open {
				lg.Trace("MetricsSink: shutting down cleanly.\n")
				return
			}
			for addr, umtx := range updateReq {
				smtx := msink.smtxMap[addr]
				if smtx == nil {
					smtx = &ServerSpanMetrics{}
					msink.smtxMap[addr] = smtx
				}
				smtx.Add(umtx)
				if lg.TraceEnabled() {
					lg.Tracef("MetricsSink: updated %s to %s\n", addr, smtx.String())
				}
			}
			msink.smtxMap.Prune(msink.maxMtx, lg)
		case accessReq := <-msink.accessReqs:
			msink.handleAccessReq(accessReq)
		}
	}
}

func (msink *MetricsSink) handleAccessReq(accessReq *AccessReq) {
	msink.lg.Debug("MetricsSink: accessing global metrics.\n")
	msink.clientDroppedLock.Lock()
	defer func() {
		msink.clientDroppedLock.Unlock()
		close(accessReq.done)
	}()
	for addr, smtx := range msink.smtxMap {
		accessReq.mtxMap[addr] = &common.SpanMetrics{
			Written:       smtx.Written,
			ServerDropped: smtx.ServerDropped,
			ClientDropped: msink.clientDroppedMap[addr],
		}
	}
}

func (msink *MetricsSink) AccessTotals() common.SpanMetricsMap {
	accessReq := &AccessReq{
		mtxMap: make(common.SpanMetricsMap),
		done:   make(chan interface{}),
	}
	msink.accessReqs <- accessReq
	<-accessReq.done
	return accessReq.mtxMap
}

func (msink *MetricsSink) UpdateMetrics(mtxMap ServerSpanMetricsMap) {
	msink.updateReqs <- mtxMap
}

func (msink *MetricsSink) Shutdown() {
	close(msink.updateReqs)
	<-msink.exited
}

func (msink *MetricsSink) UpdateClientDropped(client string, clientDropped uint64) {
	msink.clientDroppedLock.Lock()
	defer msink.clientDroppedLock.Unlock()
	msink.clientDroppedMap[client] = clientDropped
	if len(msink.clientDroppedMap) >= msink.maxMtx {
		// Delete a random entry
		for k := range msink.clientDroppedMap {
			delete(msink.clientDroppedMap, k)
			return
		}
	}
}
