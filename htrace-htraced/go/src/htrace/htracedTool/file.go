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
	"encoding/json"
	"errors"
	"fmt"
	"htrace/common"
	"io"
	"os"
)

// A file used for input.
// Transparently supports using stdin for input.
type InputFile struct {
	*os.File
	path string
}

// Open an input file.  Stdin will be used when path is -
func OpenInputFile(path string) (*InputFile, error) {
	if path == "-" {
		return &InputFile{File: os.Stdin, path: path}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &InputFile{File: file, path: path}, nil
}

func (file *InputFile) Close() {
	if file.path != "-" {
		file.File.Close()
	}
}

// A file used for output.
// Transparently supports using stdout for output.
type OutputFile struct {
	*os.File
	path string
}

// Create an output file.  Stdout will be used when path is -
func CreateOutputFile(path string) (*OutputFile, error) {
	if path == "-" {
		return &OutputFile{File: os.Stdout, path: path}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &OutputFile{File: file, path: path}, nil
}

func (file *OutputFile) Close() error {
	if file.path != "-" {
		return file.File.Close()
	}
	return nil
}

// FailureDeferringWriter is a writer which allows us to call Printf multiple
// times and then check if all the printfs succeeded at the very end, rather
// than checking after each call.   We will not attempt to write more data
// after the first write failure.
type FailureDeferringWriter struct {
	io.Writer
	err error
}

func NewFailureDeferringWriter(writer io.Writer) *FailureDeferringWriter {
	return &FailureDeferringWriter{writer, nil}
}

func (w *FailureDeferringWriter) Printf(format string, v ...interface{}) {
	if w.err != nil {
		return
	}
	str := fmt.Sprintf(format, v...)
	_, err := w.Writer.Write([]byte(str))
	if err != nil {
		w.err = err
	}
}

func (w *FailureDeferringWriter) Error() error {
	return w.err
}

// Read a file full of whitespace-separated span JSON into a slice of spans.
func readSpansFile(path string) (common.SpanSlice, error) {
	file, err := OpenInputFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readSpans(bufio.NewReader(file))
}

// Read whitespace-separated span JSON into a slice of spans.
func readSpans(reader io.Reader) (common.SpanSlice, error) {
	spans := make(common.SpanSlice, 0)
	dec := json.NewDecoder(reader)
	for {
		var span common.Span
		err := dec.Decode(&span)
		if err != nil {
			if err != io.EOF {
				return nil, errors.New(fmt.Sprintf("Decode error after decoding %d "+
					"span(s): %s", len(spans), err.Error()))
			}
			break
		}
		spans = append(spans, &span)
	}
	return spans, nil
}
