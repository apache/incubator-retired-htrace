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
	"errors"
	"htrace/common"
	"htrace/conf"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestInputFileAndOutputFile(t *testing.T) {
	tdir, err := ioutil.TempDir(os.TempDir(), "TestInputFileAndOutputFile")
	if err != nil {
		t.Fatalf("failed to create TempDir: %s\n", err.Error())
	}
	defer os.RemoveAll(tdir)
	tpath := tdir + conf.PATH_SEP + "test"
	var ofile *OutputFile
	ofile, err = CreateOutputFile(tpath)
	if err != nil {
		t.Fatalf("failed to create OutputFile at %s: %s\n", tpath, err.Error())
	}
	defer func() {
		if ofile != nil {
			ofile.Close()
		}
	}()
	w := NewFailureDeferringWriter(ofile)
	w.Printf("Hello, world!\n")
	w.Printf("2 + 2 = %d\n", 4)
	if w.Error() != nil {
		t.Fatalf("got unexpected error writing to %s: %s\n", tpath, w.Error().Error())
	}
	err = ofile.Close()
	ofile = nil
	if err != nil {
		t.Fatalf("error on closing OutputFile for %s: %s\n", tpath, err.Error())
	}
	var ifile *InputFile
	ifile, err = OpenInputFile(tpath)
	defer ifile.Close()
	expected := "Hello, world!\n2 + 2 = 4\n"
	buf := make([]byte, len(expected))
	_, err = io.ReadAtLeast(ifile, buf, len(buf))
	if err != nil {
		t.Fatalf("unexpected error on reading %s: %s\n", tpath, err.Error())
	}
	str := string(buf)
	if str != expected {
		t.Fatalf("Could not read back what we wrote to %s.\n"+
			"Got:\n%s\nExpected:\n%s\n", tpath, str, expected)
	}
}

type LimitedBufferWriter struct {
	buf []byte
	off int
}

const LIMITED_BUFFER_MESSAGE = "There isn't enough buffer to go around!"

func (w *LimitedBufferWriter) Write(p []byte) (int, error) {
	var nwritten int
	for i := range p {
		if w.off >= len(w.buf) {
			return nwritten, errors.New(LIMITED_BUFFER_MESSAGE)
		}
		w.buf[w.off] = p[i]
		w.off = w.off + 1
		nwritten++
	}
	return nwritten, nil
}

func TestFailureDeferringWriter(t *testing.T) {
	lw := LimitedBufferWriter{buf: make([]byte, 20), off: 0}
	w := NewFailureDeferringWriter(&lw)
	w.Printf("Zippity do dah #%d\n", 1)
	w.Printf("Zippity do dah #%d\n", 2)
	if w.Error() == nil {
		t.Fatalf("expected FailureDeferringWriter to experience a failure due to " +
			"limited buffer size, but it did not.")
	}
	if w.Error().Error() != LIMITED_BUFFER_MESSAGE {
		t.Fatalf("expected FailureDeferringWriter to have the error message %s, but "+
			"the message was %s\n", LIMITED_BUFFER_MESSAGE, w.Error().Error())
	}
	expected := "Zippity do dah #1\nZi"
	if string(lw.buf) != expected {
		t.Fatalf("expected LimitedBufferWriter to contain %s, but it contained %s "+
			"instead.\n", expected, string(lw.buf))
	}
}

func TestReadSpans(t *testing.T) {
	SPAN_TEST_STR := `{"a":"b9f2a1e07b6e4f16b0c2b27303b20e79",` +
		`"b":1424736225037,"e":1424736225901,"d":"ClientNamenodeProtocol#getFileInfo",` +
		`"r":"FsShell","p":["3afebdc0a13f4feb811cc5c0e42d30b1"]}
{"a":"3afebdc0a13f4feb811cc5c0e42d30b1","b":1424736224969,` +
		`"e":1424736225960,"d":"getFileInfo","r":"FsShell","p":[],"n":{"path":"/"}}
`
	r := strings.NewReader(SPAN_TEST_STR)
	spans, err := readSpans(r)
	if err != nil {
		t.Fatalf("Failed to read spans from string via readSpans: %s\n", err.Error())
	}
	SPAN_TEST_EXPECTED := common.SpanSlice{
		&common.Span{
			Id: common.TestId("b9f2a1e07b6e4f16b0c2b27303b20e79"),
			SpanData: common.SpanData{
				Begin:       1424736225037,
				End:         1424736225901,
				Description: "ClientNamenodeProtocol#getFileInfo",
				TracerId:    "FsShell",
				Parents:     []common.SpanId{common.TestId("3afebdc0a13f4feb811cc5c0e42d30b1")},
			},
		},
		&common.Span{
			Id: common.TestId("3afebdc0a13f4feb811cc5c0e42d30b1"),
			SpanData: common.SpanData{
				Begin:       1424736224969,
				End:         1424736225960,
				Description: "getFileInfo",
				TracerId:    "FsShell",
				Parents:     []common.SpanId{},
				Info: common.TraceInfoMap{
					"path": "/",
				},
			},
		},
	}
	if len(spans) != len(SPAN_TEST_EXPECTED) {
		t.Fatalf("Expected %d spans, but got %d\n",
			len(SPAN_TEST_EXPECTED), len(spans))
	}
	for i := range SPAN_TEST_EXPECTED {
		common.ExpectSpansEqual(t, spans[i], SPAN_TEST_EXPECTED[i])
	}
}
