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
	"bytes"
	"fmt"
	"htrace/conf"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
)

func InstallSignalHandlers(cnf *conf.Config) {
	fatalSigs := []os.Signal{
		os.Interrupt,
		os.Kill,
		syscall.SIGINT,
		syscall.SIGABRT,
		syscall.SIGALRM,
		syscall.SIGBUS,
		syscall.SIGFPE,
		syscall.SIGILL,
		syscall.SIGSEGV,
		syscall.SIGTERM,
	}
	fatalSigChan := make(chan os.Signal, 1)
	signal.Notify(fatalSigChan, fatalSigs...)
	lg := NewLogger("signal", cnf)
	go func() {
		sig := <-fatalSigChan
		lg.Errorf("Terminating on signal: %v\n", sig)
		lg.Close()
		os.Exit(1)
	}()

	sigQuitChan := make(chan os.Signal, 1)
	signal.Notify(sigQuitChan, syscall.SIGQUIT)
	go func() {
		stackTraceBuf := make([]byte, 1<<20)
		for {
			<-sigQuitChan
			GetStackTraces(&stackTraceBuf)
			lg.Info("=== received SIGQUIT ===\n")
			lg.Info("=== GOROUTINE STACKS ===\n")
			lg.Info(string(stackTraceBuf))
			lg.Info("\n=== END GOROUTINE STACKS ===\n")
			lg.Info("=== GC STATISTICS ===\n")
			lg.Info(GetGCStats())
			lg.Info("=== END GC STATISTICS ===\n")
		}
	}()
}

func GetStackTraces(buf *[]byte) {
	*buf = (*buf)[0:cap(*buf)]
	neededBytes := runtime.Stack(*buf, true)
	for neededBytes > len(*buf) {
		*buf = make([]byte, neededBytes)
		runtime.Stack(*buf, true)
	}
	*buf = (*buf)[0:neededBytes]
}

func GetGCStats() string {
	gcs := debug.GCStats{}
	debug.ReadGCStats(&gcs)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("LastGC: %s\n", gcs.LastGC.UTC().String()))
	buf.WriteString(fmt.Sprintf("NumGC: %d\n", gcs.NumGC))
	buf.WriteString(fmt.Sprintf("PauseTotal: %v\n", gcs.PauseTotal))
	if gcs.Pause != nil {
		pauseStr := ""
		prefix := ""
		for p := range gcs.Pause {
			pauseStr += prefix + gcs.Pause[p].String()
			prefix = ", "
		}
		buf.WriteString(fmt.Sprintf("Pause History: %s\n", pauseStr))
	}
	return buf.String()
}
