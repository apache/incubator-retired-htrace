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
	"errors"
	"fmt"
	htrace "org/apache/htrace/client"
	"org/apache/htrace/common"
	"strings"
	"unicode"
)

// Convert a string into a whitespace-separated sequence of strings.
func tokenize(str string) []string {
	prevQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == prevQuote:
			prevQuote = rune(0)
			return true
		case prevQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			prevQuote = c
			return true
		default:
			return unicode.IsSpace(c)
		}
	}
	return strings.FieldsFunc(str, f)
}

// Parses a query string in the format of a series of
// [TYPE] [OPERATOR] [CONST] tuples, joined by AND statements.
type predicateParser struct {
	tokens   []string
	curToken int
}

func (ps *predicateParser) Parse() (*common.Predicate, error) {
	if ps.curToken >= len(ps.tokens) {
		return nil, nil
	}
	if ps.curToken > 0 {
		if strings.ToLower(ps.tokens[ps.curToken]) != "and" {
			return nil, errors.New(fmt.Sprintf("Error parsing on token %d: "+
				"expected predicates to be joined by 'and', but found '%s'",
				ps.curToken, ps.tokens[ps.curToken]))
		}
		ps.curToken++
		if ps.curToken > len(ps.tokens) {
			return nil, errors.New(fmt.Sprintf("Nothing found after 'and' at "+
				"token %d", ps.curToken))
		}
	}
	field := common.Field(strings.ToLower(ps.tokens[ps.curToken]))
	if !field.IsValid() {
		return nil, errors.New(fmt.Sprintf("Invalid field specifier at token %d.  "+
			"Can't understand %s.  Valid field specifiers are %v", ps.curToken,
			ps.tokens[ps.curToken], common.ValidFields()))
	}
	ps.curToken++
	if ps.curToken > len(ps.tokens) {
		return nil, errors.New(fmt.Sprintf("Nothing found after field specifier "+
			"at token %d", ps.curToken))
	}
	op := common.Op(strings.ToLower(ps.tokens[ps.curToken]))
	if !op.IsValid() {
		return nil, errors.New(fmt.Sprintf("Invalid operation specifier at token %d.  "+
			"Can't understand %s.  Valid operation specifiers are %v", ps.curToken,
			ps.tokens[ps.curToken], common.ValidOps()))
	}
	ps.curToken++
	if ps.curToken > len(ps.tokens) {
		return nil, errors.New(fmt.Sprintf("Nothing found after field specifier "+
			"at token %d", ps.curToken))
	}
	val := ps.tokens[ps.curToken]
	ps.curToken++
	return &common.Predicate{Op: op, Field: field, Val: val}, nil
}

func parseQueryString(str string) ([]common.Predicate, error) {
	ps := predicateParser{tokens: tokenize(str)}
	if verbose {
		fmt.Printf("Running query [ ")
		prefix := ""
		for tokenIdx := range ps.tokens {
			fmt.Printf("%s'%s'", prefix, ps.tokens[tokenIdx])
			prefix = ", "
		}
		fmt.Printf(" ]\n")
	}
	preds := make([]common.Predicate, 0)
	for {
		pred, err := ps.Parse()
		if err != nil {
			return nil, err
		}
		if pred == nil {
			break
		}
		preds = append(preds, *pred)
	}
	if len(preds) == 0 {
		return nil, errors.New("Empty query string")
	}
	return preds, nil
}

// Send a query from a query string.
func doQueryFromString(hcl *htrace.Client, str string, lim int) error {
	query := &common.Query{Lim: lim}
	var err error
	query.Predicates, err = parseQueryString(str)
	if err != nil {
		return err
	}
	return doQuery(hcl, query)
}

// Send a query from a raw JSON string.
func doRawQuery(hcl *htrace.Client, str string) error {
	jsonBytes := []byte(str)
	var query common.Query
	err := json.Unmarshal(jsonBytes, &query)
	if err != nil {
		return errors.New(fmt.Sprintf("Error parsing provided JSON: %s\n", err.Error()))
	}
	return doQuery(hcl, &query)
}

// Send a query.
func doQuery(hcl *htrace.Client, query *common.Query) error {
	if verbose {
		qbytes, err := json.Marshal(*query)
		if err != nil {
			qbytes = []byte("marshaling error: " + err.Error())
		}
		fmt.Printf("Sending query: %s\n", string(qbytes))
	}
	spans, err := hcl.Query(query)
	if err != nil {
		return err
	}
	if verbose {
		fmt.Printf("%d results...\n", len(spans))
	}
	for i := range spans {
		fmt.Printf("%s\n", spans[i].ToJson())
	}
	return nil
}
