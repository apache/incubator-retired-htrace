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

package test

import (
	"math/rand"
	"org/apache/htrace/common"
)

func NonZeroRand64(rnd *rand.Rand) int64 {
	for {
		r := rnd.Int63()
		if r == 0 {
			continue
		}
		if rnd.Intn(1) != 0 {
			return -r
		}
		return r
	}
}

func NonZeroRand32(rnd *rand.Rand) int32 {
	for {
		r := rnd.Int31()
		if r == 0 {
			continue
		}
		if rnd.Intn(1) != 0 {
			return -r
		}
		return r
	}
}

// Create a random span.
func NewRandomSpan(rnd *rand.Rand, potentialParents []*common.Span) *common.Span {
	var parentId int64 = common.INVALID_SPAN_ID
	if potentialParents != nil {
		parentIdx := rnd.Intn(len(potentialParents) + 1)
		if parentIdx < len(potentialParents) {
			parentId = potentialParents[parentIdx].SpanId
		}
	}
	return &common.Span{SpanId: NonZeroRand64(rnd),
		SpanData: common.SpanData{
			Start:       NonZeroRand64(rnd),
			Stop:        NonZeroRand64(rnd),
			Description: "getFileDescriptors",
			TraceId:     NonZeroRand64(rnd),
			ParentId:    parentId,
			ProcessId:   NonZeroRand32(rnd),
		}}
}
