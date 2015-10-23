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
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"reflect"
	"testing"
	"time"
)

func TestMetricsSinkStartupShutdown(t *testing.T) {
	cnfBld := conf.Builder{
		Values:   conf.TEST_VALUES(),
		Defaults: conf.DEFAULTS,
	}
	cnf, err := cnfBld.Build()
	if err != nil {
		t.Fatalf("failed to create conf: %s", err.Error())
	}
	msink := NewMetricsSink(cnf)
	msink.Shutdown()
}

func TestAddSpanMetrics(t *testing.T) {
	a := &ServerSpanMetrics{
		Written:       100,
		ServerDropped: 200,
	}
	b := &ServerSpanMetrics{
		Written:       500,
		ServerDropped: 100,
	}
	a.Add(b)
	if a.Written != 600 {
		t.Fatalf("SpanMetrics#Add failed to update #Written")
	}
	if a.ServerDropped != 300 {
		t.Fatalf("SpanMetrics#Add failed to update #Dropped")
	}
	if b.Written != 500 {
		t.Fatalf("SpanMetrics#Add updated b#Written")
	}
	if b.ServerDropped != 100 {
		t.Fatalf("SpanMetrics#Add updated b#Dropped")
	}
}

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

func waitForMetrics(msink *MetricsSink, expectedTotals common.SpanMetricsMap) {
	for {
		time.Sleep(1 * time.Millisecond)
		totals := msink.AccessTotals()
		if compareTotals(totals, expectedTotals) {
			return
		}
	}
}

func TestMetricsSinkMessages(t *testing.T) {
	cnfBld := conf.Builder{
		Values:   conf.TEST_VALUES(),
		Defaults: conf.DEFAULTS,
	}
	cnf, err := cnfBld.Build()
	if err != nil {
		t.Fatalf("failed to create conf: %s", err.Error())
	}
	msink := NewMetricsSink(cnf)
	totals := msink.AccessTotals()
	if len(totals) != 0 {
		t.Fatalf("Expected no data in the MetricsSink to start with.")
	}
	msink.UpdateMetrics(ServerSpanMetricsMap{
		"192.168.0.100": &ServerSpanMetrics{
			Written:       20,
			ServerDropped: 10,
		},
	})
	waitForMetrics(msink, common.SpanMetricsMap{
		"192.168.0.100": &common.SpanMetrics{
			Written:       20,
			ServerDropped: 10,
		},
	})
	msink.UpdateMetrics(ServerSpanMetricsMap{
		"192.168.0.100": &ServerSpanMetrics{
			Written:       200,
			ServerDropped: 100,
		},
	})
	msink.UpdateMetrics(ServerSpanMetricsMap{
		"192.168.0.100": &ServerSpanMetrics{
			Written:       1000,
			ServerDropped: 1000,
		},
	})
	waitForMetrics(msink, common.SpanMetricsMap{
		"192.168.0.100": &common.SpanMetrics{
			Written:       1220,
			ServerDropped: 1110,
		},
	})
	msink.UpdateMetrics(ServerSpanMetricsMap{
		"192.168.0.200": &ServerSpanMetrics{
			Written:       200,
			ServerDropped: 100,
		},
	})
	waitForMetrics(msink, common.SpanMetricsMap{
		"192.168.0.100": &common.SpanMetrics{
			Written:       1220,
			ServerDropped: 1110,
		},
		"192.168.0.200": &common.SpanMetrics{
			Written:       200,
			ServerDropped: 100,
		},
	})
	msink.Shutdown()
}

func TestMetricsSinkMessagesEviction(t *testing.T) {
	cnfBld := conf.Builder{
		Values:   conf.TEST_VALUES(),
		Defaults: conf.DEFAULTS,
	}
	cnfBld.Values[conf.HTRACE_METRICS_MAX_ADDR_ENTRIES] = "2"
	cnfBld.Values[conf.HTRACE_METRICS_HEARTBEAT_PERIOD_MS] = "1"
	cnf, err := cnfBld.Build()
	if err != nil {
		t.Fatalf("failed to create conf: %s", err.Error())
	}
	msink := NewMetricsSink(cnf)
	msink.UpdateMetrics(ServerSpanMetricsMap{
		"192.168.0.100": &ServerSpanMetrics{
			Written:       20,
			ServerDropped: 10,
		},
		"192.168.0.101": &ServerSpanMetrics{
			Written:       20,
			ServerDropped: 10,
		},
		"192.168.0.102": &ServerSpanMetrics{
			Written:       20,
			ServerDropped: 10,
		},
	})
	for {
		totals := msink.AccessTotals()
		if len(totals) == 2 {
			break
		}
	}
	msink.Shutdown()
}
