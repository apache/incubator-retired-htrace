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
	"sync/atomic"
	"testing"
	"time"
)

func TestSemaphoreWake(t *testing.T) {
	var done uint32
	sem := NewSemaphore(0)
	go func() {
		time.Sleep(10 * time.Nanosecond)
		atomic.AddUint32(&done, 1)
		sem.Post()
	}()
	sem.Wait()
	doneVal := atomic.LoadUint32(&done)
	if doneVal != 1 {
		t.Fatalf("sem.Wait did not wait for sem.Post")
	}
}

func TestSemaphoreCount(t *testing.T) {
	sem := NewSemaphore(1)
	sem.Post()
	sem.Wait()
	sem.Wait()

	sem = NewSemaphore(-1)
	sem.Post()
	sem.Post()
	sem.Wait()
}

func TestSemaphoreMultipleGoroutines(t *testing.T) {
	var done uint32
	sem := NewSemaphore(0)
	sem2 := NewSemaphore(0)
	go func() {
		sem.Wait()
		atomic.AddUint32(&done, 1)
		sem2.Post()
	}()
	go func() {
		time.Sleep(10 * time.Nanosecond)
		atomic.AddUint32(&done, 1)
		sem.Post()
	}()
	go func() {
		time.Sleep(20 * time.Nanosecond)
		atomic.AddUint32(&done, 1)
		sem.Post()
	}()
	sem.Wait()
	go func() {
		time.Sleep(10 * time.Nanosecond)
		atomic.AddUint32(&done, 1)
		sem.Post()
	}()
	sem.Wait()
	sem2.Wait()
	doneVal := atomic.LoadUint32(&done)
	if doneVal != 4 {
		t.Fatalf("sem.Wait did not wait for sem.Posts")
	}
}
