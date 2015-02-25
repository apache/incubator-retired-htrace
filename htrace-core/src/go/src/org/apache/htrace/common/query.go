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
	"encoding/json"
)

//
// Represents queries that can be sent to htraced.
//
// Each query consists of set of predicates that will be 'AND'ed together to
// return a set of spans.  Predicates contain an operation, a field, and a
// value.
//
// For example, a query might be "return the first 100 spans between 5:00pm
// and 5:01pm"  This query would have two predicates: time greater than or
// equal to 5:00pm, and time less than or equal to 5:01pm.
// In HTrace, times are always expressed in milliseconds since the Epoch.
// So this would become:
// { "lim" : 100, "pred" : [
//   { "op" : "ge", "field" : "begin", "val" : 1234 },
//   { "op" : "le", "field" : "begin", "val" : 5678 },
// ] }
//
// Where '1234' and '5678' were replaced by times since the epoch in
// milliseconds.
//

type Op string

const (
	CONTAINS               Op = "cn"
	EQUALS                 Op = "eq"
	LESS_THAN_OR_EQUALS    Op = "le"
	GREATER_THAN_OR_EQUALS Op = "ge"
	GREATER_THAN           Op = "gt"
)

func (op Op) IsDescending() bool {
	return op == LESS_THAN_OR_EQUALS
}

func (op Op) IsValid() bool {
	ops := ValidOps()
	for i := range ops {
		if ops[i] == op {
			return true
		}
	}
	return false
}

func ValidOps() []Op {
	return []Op{CONTAINS, EQUALS, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS,
		GREATER_THAN}
}

type Field string

const (
	SPAN_ID     Field = "spanid"
	DESCRIPTION Field = "description"
	BEGIN_TIME  Field = "begin"
	END_TIME    Field = "end"
	DURATION    Field = "duration"
)

func (field Field) IsValid() bool {
	fields := ValidFields()
	for i := range fields {
		if fields[i] == field {
			return true
		}
	}
	return false
}

func ValidFields() []Field {
	return []Field{SPAN_ID, DESCRIPTION, BEGIN_TIME, END_TIME, DURATION}
}

type Predicate struct {
	Op    Op     `json:"op"`
	Field Field  `json:"field"`
	Val   string `val:"val"`
}

type Query struct {
	Predicates []Predicate `json:"pred"`
	Lim        int         `json:"lim"`
}

func (query *Query) String() string {
	buf, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}
	return string(buf)
}
