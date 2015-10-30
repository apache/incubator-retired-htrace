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
	"sync"
	"time"
)

type Heartbeater struct {
	// The name of this heartbeater
	name string

	// How long to sleep between heartbeats, in milliseconds.
	periodMs int64

	// The logger to use.
	lg *common.Logger

	// The channels to send the heartbeat on.
	targets []HeartbeatTarget

	// Incoming requests to the heartbeater.  When this is closed, the
	// heartbeater will exit.
	req chan *HeartbeatTarget

	wg sync.WaitGroup
}

type HeartbeatTarget struct {
	// The name of the heartbeat target.
	name string

	// The channel for the heartbeat target.
	targetChan chan interface{}
}

func (tgt *HeartbeatTarget) String() string {
	return tgt.name
}

func NewHeartbeater(name string, periodMs int64, lg *common.Logger) *Heartbeater {
	hb := &Heartbeater{
		name:     name,
		periodMs: periodMs,
		lg:       lg,
		targets:  make([]HeartbeatTarget, 0, 4),
		req:      make(chan *HeartbeatTarget),
	}
	hb.wg.Add(1)
	go hb.run()
	return hb
}

func (hb *Heartbeater) AddHeartbeatTarget(tgt *HeartbeatTarget) {
	hb.req <- tgt
}

func (hb *Heartbeater) Shutdown() {
	close(hb.req)
	hb.wg.Wait()
}

func (hb *Heartbeater) String() string {
	return hb.name
}

func (hb *Heartbeater) run() {
	defer func() {
		hb.lg.Debugf("%s: exiting.\n", hb.String())
		hb.wg.Done()
	}()
	period := time.Duration(hb.periodMs) * time.Millisecond
	for {
		periodEnd := time.Now().Add(period)
		for {
			timeToWait := periodEnd.Sub(time.Now())
			if timeToWait <= 0 {
				break
			} else if timeToWait > period {
				// Smooth over jitter or clock changes
				timeToWait = period
				periodEnd = time.Now().Add(period)
			}
			select {
			case tgt, open := <-hb.req:
				if !open {
					return
				}
				hb.targets = append(hb.targets, *tgt)
				hb.lg.Debugf("%s: added %s.\n", hb.String(), tgt.String())
			case <-time.After(timeToWait):
			}
		}
		for targetIdx := range hb.targets {
			select {
			case hb.targets[targetIdx].targetChan <- nil:
			default:
				// We failed to send a heartbeat because the other goroutine was busy and
				// hasn't cleared the previous one from its channel.  This could indicate a
				// stuck goroutine.
				hb.lg.Infof("%s: could not send heartbeat to %s.\n",
					hb.String(), hb.targets[targetIdx])
			}
		}
	}
}
