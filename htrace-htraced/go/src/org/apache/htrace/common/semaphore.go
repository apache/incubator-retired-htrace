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

package common

import (
	"sync"
)

// A simple lock-and-condition-variable based semaphore implementation.
type Semaphore struct {
	lock sync.Mutex
	cond *sync.Cond
	count int64
}

func NewSemaphore(count int64) *Semaphore {
	sem := &Semaphore {
		count:int64(count),
	}
	sem.cond = &sync.Cond {
		L: &sem.lock,
	}
	return sem
}

func (sem *Semaphore) Post() {
	sem.lock.Lock()
	sem.count++
	if sem.count > 0 {
		sem.cond.Broadcast()
	}
	sem.lock.Unlock()
}

func (sem *Semaphore) Posts(amt int64) {
	sem.lock.Lock()
	sem.count+=amt
	if sem.count > 0 {
		sem.cond.Broadcast()
	}
	sem.lock.Unlock()
}

func (sem *Semaphore) Wait() {
	sem.lock.Lock()
	for {
		if sem.count > 0 {
			sem.count--
			sem.lock.Unlock()
			return
		}
		sem.cond.Wait()
	}
}

func (sem *Semaphore) Waits(amt int64) {
	var i int64
	for i=0; i<amt; i++ {
		sem.Wait()
	}
}
