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
	"fmt"
	"github.com/alecthomas/kingpin"
	"github.com/jmhodges/levigo"
	"htrace/common"
	"htrace/conf"
	"net"
	"os"
	"runtime"
	"time"
)

var RELEASE_VERSION string
var GIT_VERSION string

const USAGE = `htraced: the HTrace server daemon.

htraced receives trace spans sent from HTrace clients.  It exposes a REST
interface which others can query.  It also runs a web server with a graphical
user interface.  htraced stores its span data in levelDB files on the local
disks.

Usage:
--help: this help message

-Dk=v: set configuration key 'k' to value 'v'
For example -Dweb.address=127.0.0.1:8080 sets the web address to localhost,
port 8080.  -Dlog.level=DEBUG will set the default log level to DEBUG.

-Dk: set configuration key 'k' to 'true'

Normally, configuration options should be set in the ` + conf.CONFIG_FILE_NAME + `
configuration file.  We find this file by searching the paths in the
` + conf.HTRACED_CONF_DIR + `. The command-line options are just an alternate way
of setting configuration when launching the daemon.
`

func main() {
	// Load the htraced configuration.
	// This also parses the -Dfoo=bar command line arguments and removes them
	// from os.Argv.
	cnf, cnfLog := conf.LoadApplicationConfig("htraced.")

	// Parse the remaining command-line arguments.
	app := kingpin.New(os.Args[0], USAGE)
	version := app.Command("version", "Print server version and exit.")
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// Handle the "version" command-line argument.
	if cmd == version.FullCommand() {
		fmt.Printf("Running htraced %s [%s].\n", RELEASE_VERSION, GIT_VERSION)
		os.Exit(0)
	}

	// Open the HTTP port.
	// We want to do this first, before initializing the datastore or setting up
	// logging.  That way, if someone accidentally starts two daemons with the
	// same config file, the second invocation will exit with a "port in use"
	// error rather than potentially disrupting the first invocation.
	rstListener, listenErr := net.Listen("tcp", cnf.Get(conf.HTRACE_WEB_ADDRESS))
	if listenErr != nil {
		fmt.Fprintf(os.Stderr, "Error opening HTTP port: %s\n",
			listenErr.Error())
		os.Exit(1)
	}

	// Print out the startup banner and information about the daemon
	// configuration.
	lg := common.NewLogger("main", cnf)
	defer lg.Close()
	lg.Infof("*** Starting htraced %s [%s]***\n", RELEASE_VERSION, GIT_VERSION)
	scanner := bufio.NewScanner(cnfLog)
	for scanner.Scan() {
		lg.Infof(scanner.Text() + "\n")
	}
	common.InstallSignalHandlers(cnf)
	if runtime.GOMAXPROCS(0) == 1 {
		ncpu := runtime.NumCPU()
		runtime.GOMAXPROCS(ncpu)
		lg.Infof("setting GOMAXPROCS=%d\n", ncpu)
	} else {
		lg.Infof("GOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))
	}
	lg.Infof("leveldb version=%d.%d\n",
		levigo.GetLevelDBMajorVersion(), levigo.GetLevelDBMinorVersion())

	// Initialize the datastore.
	store, err := CreateDataStore(cnf, nil)
	if err != nil {
		lg.Errorf("Error creating datastore: %s\n", err.Error())
		os.Exit(1)
	}
	var rsv *RestServer
	rsv, err = CreateRestServer(cnf, store, rstListener)
	if err != nil {
		lg.Errorf("Error creating REST server: %s\n", err.Error())
		os.Exit(1)
	}
	var hsv *HrpcServer
	if cnf.Get(conf.HTRACE_HRPC_ADDRESS) != "" {
		hsv, err = CreateHrpcServer(cnf, store, nil)
		if err != nil {
			lg.Errorf("Error creating HRPC server: %s\n", err.Error())
			os.Exit(1)
		}
	} else {
		lg.Infof("Not starting HRPC server because no value was given for %s.\n",
			conf.HTRACE_HRPC_ADDRESS)
	}
	naddr := cnf.Get(conf.HTRACE_STARTUP_NOTIFICATION_ADDRESS)
	if naddr != "" {
		notif := StartupNotification{
			HttpAddr:  rsv.Addr().String(),
			ProcessId: os.Getpid(),
		}
		if hsv != nil {
			notif.HrpcAddr = hsv.Addr().String()
		}
		err = sendStartupNotification(naddr, &notif)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send startup notification: "+
				"%s\n", err.Error())
			os.Exit(1)
		}
	}
	for {
		time.Sleep(time.Duration(10) * time.Hour)
	}
}

// A startup notification message that we optionally send on startup.
// Used by unit tests.
type StartupNotification struct {
	HttpAddr  string
	HrpcAddr  string
	ProcessId int
}

func sendStartupNotification(naddr string, notif *StartupNotification) error {
	conn, err := net.Dial("tcp", naddr)
	if err != nil {
		return err
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	var buf []byte
	buf, err = json.Marshal(notif)
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	conn.Close()
	conn = nil
	return nil
}
