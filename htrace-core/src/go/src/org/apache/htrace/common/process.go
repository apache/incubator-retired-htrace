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
		syscall.SIGQUIT,
		syscall.SIGSEGV,
		syscall.SIGTERM,
	}
	sigChan := make(chan os.Signal, len(fatalSigs))
	signal.Notify(sigChan, fatalSigs...)
	lg := NewLogger("exit", cnf)
	go func() {
		sig := <-sigChan
		lg.Errorf("Terminating on signal: %v\n", sig)
		lg.Close()
		os.Exit(1)
	}()
}
