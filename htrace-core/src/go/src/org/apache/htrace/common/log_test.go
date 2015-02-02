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
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"org/apache/htrace/conf"
	"os"
	"strings"
	"testing"
)

func newLogger(faculty string, args ...string) *Logger {
	cnfBld := conf.Builder{Defaults: conf.DEFAULTS}
	cnf, err := cnfBld.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to create conf: %s", err.Error()))
	}
	cnf2 := cnf.Clone(args...)
	lg := NewLogger(faculty, cnf2)
	return lg
}

func TestNewLogger(t *testing.T) {
	lg := newLogger("foo", "log.level", "TRACE")
	lg.Close()
}

func verifyLines(t *testing.T, rdr io.Reader, lines []string) {
	scanner := bufio.NewScanner(rdr)
	lineIdx := 0
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, lines[lineIdx]) {
			t.Fatalf("Error on line %d: didn't find substring '%s' in line '%s'\n",
				(lineIdx + 1), lines[lineIdx], line)
		}
		lineIdx++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestFileLogs(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "testFileLogs")
	if err != nil {
		panic(fmt.Sprintf("error creating tempdir: %s\n", err.Error()))
	}
	defer os.RemoveAll(tempDir)
	logPath := tempDir + conf.PATH_SEP + "log"
	lg := newLogger("foo", "log.level", "DEBUG",
		"foo.log.level", "INFO",
		"log.path", logPath)
	lg.Tracef("Non-important stuff, ignore this.\n")
	lg.Infof("problem with the foobar\n")
	lg.Tracef("More non-important stuff, also ignore this.\n")
	lg.Infof("and another problem with the foobar\n")
	logFile, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("failed to open file %s: %s\n", logPath, err.Error())
	}
	verifyLines(t, logFile, []string{
		"problem with the foobar",
		"and another problem with the foobar",
	})
	logFile.Close()
	lg.Close()
}

func TestMultipleFileLogs(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "testMultipleFileLogs")
	if err != nil {
		panic(fmt.Sprintf("error creating tempdir: %s\n", err.Error()))
	}
	defer os.RemoveAll(tempDir)
	logPath := tempDir + conf.PATH_SEP + "log"
	fooLg := newLogger("foo", "log.level", "DEBUG",
		"foo.log.level", "INFO",
		"log.path", logPath)
	fooLg.Infof("The foo needs maintenance.\n")
	barLg := newLogger("bar", "log.level", "DEBUG",
		"foo.log.level", "INFO",
		"log.path", logPath)
	barLg.Debugf("The bar is open\n")
	fooLg.Errorf("Fizz buzz\n")
	logFile, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("failed to open file %s: %s\n", logPath, err.Error())
	}
	fooLg.Tracef("Fizz buzz2\n")
	barLg.Tracef("Fizz buzz3\n")
	verifyLines(t, logFile, []string{
		"The foo needs maintenance.",
		"The bar is open",
		"Fizz buzz",
		"Fizz buzz3",
	})
	logFile.Close()
	fooLg.Close()
	barLg.Close()
}
