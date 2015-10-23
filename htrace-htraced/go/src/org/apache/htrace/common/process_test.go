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
	"org/apache/htrace/conf"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

const HTRACED_TEST_HELPER_PROCESS = "HTRACED_TEST_HELPER_PROCESS"

// This test runs a helper process which will install our htraced signal
// handlers.  We will send signals to the subprocess and verify that it has
// caught them and responded appropriately.
func TestSignals(t *testing.T) {
	if os.Getenv(HTRACED_TEST_HELPER_PROCESS) == "1" {
		runHelperProcess()
		os.Exit(0)
	}
	helper := exec.Command(os.Args[0], "-test.run=TestSignals", "--")
	helper.Env = []string{HTRACED_TEST_HELPER_PROCESS + "=1"}
	stdoutPipe, err := helper.StdoutPipe()
	if err != nil {
		panic(fmt.Sprintf("Failed to open pipe to process stdout: %s",
			err.Error()))
	}
	stderrPipe, err := helper.StderrPipe()
	if err != nil {
		panic(fmt.Sprintf("Failed to open pipe to process stderr: %s",
			err.Error()))
	}
	err = helper.Start()
	if err != nil {
		t.Fatal("Failed to start command %s: %s\n", os.Args[0], err.Error())
	}
	t.Logf("Started suprocess...\n")
	done := make(chan interface{})
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			text := scanner.Text()
			if strings.Contains(text, "=== GOROUTINE STACKS ===") {
				break
			}
		}
		t.Logf("Saw 'GOROUTINE STACKS on stdout.'  Sending SIGINT.\n")
		helper.Process.Signal(syscall.SIGINT)
		for scanner.Scan() {
			text := scanner.Text()
			if strings.Contains(text, "Terminating on signal: SIGINT") {
				break
			}
		}
		t.Logf("Saw 'Terminating on signal: SIGINT'.  " +
			"Helper goroutine exiting.\n")
		done <- nil
	}()
	scanner := bufio.NewScanner(stderrPipe)
	for scanner.Scan() {
		text := scanner.Text()
		if strings.Contains(text, "Signal handler installed.") {
			break
		}
	}
	t.Logf("Saw 'Signal handler installed.'  Sending SIGINT.")
	helper.Process.Signal(syscall.SIGQUIT)
	t.Logf("Waiting for helper goroutine to exit.\n")
	<-done
	t.Logf("Waiting for subprocess to exit.\n")
	helper.Wait()
	t.Logf("Done.")
}

// Run the helper process which TestSignals spawns.
func runHelperProcess() {
	cnfMap := map[string]string{
		conf.HTRACE_LOG_LEVEL: "TRACE",
		conf.HTRACE_LOG_PATH:  "", // log to stdout
	}
	cnfBld := conf.Builder{Values: cnfMap, Defaults: conf.DEFAULTS}
	cnf, err := cnfBld.Build()
	if err != nil {
		fmt.Printf("Error building configuration: %s\n", err.Error())
		os.Exit(1)
	}
	InstallSignalHandlers(cnf)
	fmt.Fprintf(os.Stderr, "Signal handler installed.\n")
	// Wait for a signal to be delivered
	for {
		time.Sleep(time.Hour * 100)
	}
}
