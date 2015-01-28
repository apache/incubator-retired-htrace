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
	"fmt"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"strings"
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
port 8080.

-Dk: set configuration key 'k' to 'true'

Normally, configuration options should be set in the ` + conf.CONFIG_FILE_NAME + `
configuration file.  We find this file by searching the paths in the 
` + conf.HTRACED_CONF_DIR + `. The command-line options are just an alternate way
of setting configuration when launching the daemon.
`

func main() {
	for idx := range os.Args {
		arg := os.Args[idx]
		if strings.HasPrefix(arg, "--h") || strings.HasPrefix(arg, "-h") {
			fmt.Fprintf(os.Stderr, USAGE)
			os.Exit(0)
		}
	}
	cnf := conf.LoadApplicationConfig()
	lg := common.NewLogger("main", cnf)
	defer lg.Close()
	store, err := CreateDataStore(cnf, nil)
	if err != nil {
		lg.Errorf("Error creating datastore: %s\n", err.Error())
		os.Exit(1)
	}
	_, err = CreateRestServer(cnf, store)
	if err != nil {
		lg.Errorf("Error creating REST server: %s\n", err.Error())
		os.Exit(1)
	}
	for {
		time.Sleep(time.Duration(10) * time.Hour)
	}
}
