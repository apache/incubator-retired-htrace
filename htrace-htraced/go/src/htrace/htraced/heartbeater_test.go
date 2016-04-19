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
	"htrace/common"
	"htrace/conf"
	"testing"
	"time"
)

func TestHeartbeaterStartupShutdown(t *testing.T) {
	cnfBld := conf.Builder{
		Values:   conf.TEST_VALUES(),
		Defaults: conf.DEFAULTS,
	}
	cnf, err := cnfBld.Build()
	if err != nil {
		t.Fatalf("failed to create conf: %s", err.Error())
	}
	lg := common.NewLogger("heartbeater", cnf)
	hb := NewHeartbeater("ExampleHeartbeater", 1, lg)
	if hb.String() != "ExampleHeartbeater" {
		t.Fatalf("hb.String() returned %s instead of %s\n", hb.String(), "ExampleHeartbeater")
	}
	hb.Shutdown()
}

// The number of milliseconds between heartbeats
const HEARTBEATER_PERIOD = 5

// The number of heartbeats to send in the test.
const NUM_TEST_HEARTBEATS = 3

func TestHeartbeaterSendsHeartbeats(t *testing.T) {
	cnfBld := conf.Builder{
		Values:   conf.TEST_VALUES(),
		Defaults: conf.DEFAULTS,
	}
	cnf, err := cnfBld.Build()
	if err != nil {
		t.Fatalf("failed to create conf: %s", err.Error())
	}
	lg := common.NewLogger("heartbeater", cnf)
	// The minimum amount of time which the heartbeater test should take
	MINIMUM_TEST_DURATION := time.Millisecond * (NUM_TEST_HEARTBEATS * HEARTBEATER_PERIOD)
	duration := MINIMUM_TEST_DURATION
	for duration <= MINIMUM_TEST_DURATION {
		start := time.Now()
		testHeartbeaterSendsHeartbeatsImpl(t, lg)
		end := time.Now()
		duration = end.Sub(start)
		lg.Debugf("Measured duration: %v; minimum expected duration: %v\n",
			duration, MINIMUM_TEST_DURATION)
	}
}

func testHeartbeaterSendsHeartbeatsImpl(t *testing.T, lg *common.Logger) {
	hb := NewHeartbeater("ExampleHeartbeater", HEARTBEATER_PERIOD, lg)
	if hb.String() != "ExampleHeartbeater" {
		t.Fatalf("hb.String() returned %s instead of %s\n", hb.String(), "ExampleHeartbeater")
	}
	testChan := make(chan interface{}, NUM_TEST_HEARTBEATS)
	gotAllHeartbeats := make(chan bool)
	hb.AddHeartbeatTarget(&HeartbeatTarget{
		name:       "ExampleHeartbeatTarget",
		targetChan: testChan,
	})
	go func() {
		for i := 0; i < NUM_TEST_HEARTBEATS; i++ {
			<-testChan
		}
		gotAllHeartbeats <- true
		for i := 0; i < NUM_TEST_HEARTBEATS; i++ {
			_, open := <-testChan
			if !open {
				return
			}
		}
	}()
	<-gotAllHeartbeats
	hb.Shutdown()
}
