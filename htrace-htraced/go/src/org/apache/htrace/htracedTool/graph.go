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
	"bufio"
	"errors"
	"fmt"
	"io"
	"org/apache/htrace/common"
	"os"
	"sort"
)

// Create a dotfile from a json file.
func jsonSpanFileToDotFile(jsonFile string, dotFile string) error {
	spans, err := readSpansFile(jsonFile)
	if err != nil {
		return errors.New(fmt.Sprintf("error reading %s: %s",
			jsonFile, err.Error()))
	}
	var file *OutputFile
	file, err = CreateOutputFile(dotFile)
	if err != nil {
		return errors.New(fmt.Sprintf("error opening %s for write: %s",
			dotFile, err.Error()))
	}
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	writer := bufio.NewWriter(file)
	err = spansToDot(spans, writer)
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	err = file.Close()
	file = nil
	return err
}

// Create output in dotfile format from a set of spans.
func spansToDot(spans common.SpanSlice, writer io.Writer) error {
	sort.Sort(spans)
	idMap := make(map[[16]byte]*common.Span)
	for i := range spans {
		span := spans[i]
		if idMap[span.Id.ToArray()] != nil {
			fmt.Fprintf(os.Stderr, "There were multiple spans listed which "+
				"had ID %s.\nFirst:%s\nOther:%s\n", span.Id.String(),
				idMap[span.Id.ToArray()].ToJson(), span.ToJson())
		} else {
			idMap[span.Id.ToArray()] = span
		}
	}
	childMap := make(map[[16]byte]common.SpanSlice)
	for i := range spans {
		child := spans[i]
		for j := range child.Parents {
			parent := idMap[child.Parents[j].ToArray()]
			if parent == nil {
				fmt.Fprintf(os.Stderr, "Can't find parent id %s for %s\n",
					child.Parents[j].String(), child.ToJson())
			} else {
				children := childMap[parent.Id.ToArray()]
				if children == nil {
					children = make(common.SpanSlice, 0)
				}
				children = append(children, child)
				childMap[parent.Id.ToArray()] = children
			}
		}
	}
	w := NewFailureDeferringWriter(writer)
	w.Printf("digraph spans {\n")
	// Write out the nodes with their descriptions.
	for i := range spans {
		w.Printf(fmt.Sprintf(`  "%s" [label="%s"];`+"\n",
			spans[i].Id.String(), spans[i].Description))
	}
	// Write out the edges between nodes... the parent/children relationships
	for i := range spans {
		children := childMap[spans[i].Id.ToArray()]
		sort.Sort(children)
		if children != nil {
			for c := range children {
				w.Printf(fmt.Sprintf(`  "%s" -> "%s";`+"\n",
					spans[i].Id.String(), children[c].Id))
			}
		}
	}
	w.Printf("}\n")
	return w.Error()
}
