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
	"org/apache/htrace/conf"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
)

func LoadApplicationConfig() *conf.Config {
	cnf, dlog := conf.LoadApplicationConfig()
	lg := NewLogger("conf", cnf)
	defer lg.Close()
	if lg.Level <= DEBUG {
		// Print out the debug information from loading the configuration.
		scanner := bufio.NewScanner(dlog)
		for scanner.Scan() {
			lg.Debugf(scanner.Text() + "\n")
		}
	}
	return cnf
}

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
		bufSize := 1 << 20
		buf := make([]byte, bufSize)
		for {
			<-sigQuitChan
			neededBytes := runtime.Stack(buf, true)
			if neededBytes > bufSize {
				bufSize = neededBytes
				buf = make([]byte, bufSize)
				runtime.Stack(buf, true)
			}
			lg.Info("=== received SIGQUIT ===\n")
			lg.Info("=== GOROUTINE STACKS ===\n")
			lg.Info(string(buf[:neededBytes]))
			lg.Info("\n=== END GOROUTINE STACKS ===\n")
			gcs := debug.GCStats{}
			debug.ReadGCStats(&gcs)
			lg.Info("=== GC STATISTICS ===\n")
			lg.Infof("LastGC: %s\n", gcs.LastGC.UTC().String())
			lg.Infof("NumGC: %d\n", gcs.NumGC)
			lg.Infof("PauseTotal: %v\n", gcs.PauseTotal)
			if gcs.Pause != nil {
				pauseStr := ""
				prefix := ""
				for p := range gcs.Pause {
					pauseStr += prefix + gcs.Pause[p].String()
					prefix = ", "
				}
				lg.Infof("Pause History: %s\n", pauseStr)
			}
			lg.Info("=== END GC STATISTICS ===\n")
		}
	}()
}
