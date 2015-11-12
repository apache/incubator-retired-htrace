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

package conf

import (
	"fmt"
	"os"
)

//
// Configuration keys for HTrace.
//

// The platform-specific path separator.  Usually slash.
var PATH_SEP string = fmt.Sprintf("%c", os.PathSeparator)

// The platform-specific path list separator.  Usually colon.
var PATH_LIST_SEP string = fmt.Sprintf("%c", os.PathListSeparator)

// The name of the XML configuration file to look for.
const CONFIG_FILE_NAME = "htraced-conf.xml"

// An environment variable containing a list of paths to search for the
// configuration file in.
const HTRACED_CONF_DIR = "HTRACED_CONF_DIR"

// The web address to start the REST server on.
const HTRACE_WEB_ADDRESS = "web.address"

// The default port for the Htrace web address.
const HTRACE_WEB_ADDRESS_DEFAULT_PORT = 9095

// The web address to start the REST server on.
const HTRACE_HRPC_ADDRESS = "hrpc.address"

// The default port for the Htrace HRPC address.
const HTRACE_HRPC_ADDRESS_DEFAULT_PORT = 9075

// The directories to put the data store into.  Separated by PATH_LIST_SEP.
const HTRACE_DATA_STORE_DIRECTORIES = "data.store.directories"

// Boolean key which indicates whether we should clear data on startup.
const HTRACE_DATA_STORE_CLEAR = "data.store.clear"

// How many writes to buffer before applying backpressure to span senders.
const HTRACE_DATA_STORE_SPAN_BUFFER_SIZE = "data.store.span.buffer.size"

// Path to put the logs from htrace, or the empty string to use stdout.
const HTRACE_LOG_PATH = "log.path"

// The log level to use for the logs in htrace.
const HTRACE_LOG_LEVEL = "log.level"

// The period between metrics heartbeats.  This is the approximate interval at which we will
// update global metrics.
const HTRACE_METRICS_HEARTBEAT_PERIOD_MS = "metrics.heartbeat.period.ms"

// The maximum number of addresses for which we will maintain metrics.
const HTRACE_METRICS_MAX_ADDR_ENTRIES = "metrics.max.addr.entries"

// The number of milliseconds we should keep spans before discarding them.
const HTRACE_SPAN_EXPIRY_MS = "span.expiry.ms"

// The period between updates to the span reaper
const HTRACE_REAPER_HEARTBEAT_PERIOD_MS = "reaper.heartbeat.period.ms"

// A host:port pair to send information to on startup.  This is used in unit
// tests to determine the (random) port of the htraced process that has been
// started.
const HTRACE_STARTUP_NOTIFICATION_ADDRESS = "startup.notification.address"

// Default values for HTrace configuration keys.
var DEFAULTS = map[string]string{
	HTRACE_WEB_ADDRESS:  fmt.Sprintf("0.0.0.0:%d", HTRACE_WEB_ADDRESS_DEFAULT_PORT),
	HTRACE_HRPC_ADDRESS: fmt.Sprintf("0.0.0.0:%d", HTRACE_HRPC_ADDRESS_DEFAULT_PORT),
	HTRACE_DATA_STORE_DIRECTORIES: PATH_SEP + "tmp" + PATH_SEP + "htrace1" +
		PATH_LIST_SEP + PATH_SEP + "tmp" + PATH_SEP + "htrace2",
	HTRACE_DATA_STORE_CLEAR:            "false",
	HTRACE_DATA_STORE_SPAN_BUFFER_SIZE: "100",
	HTRACE_LOG_PATH:                    "",
	HTRACE_LOG_LEVEL:                   "INFO",
	HTRACE_METRICS_HEARTBEAT_PERIOD_MS: fmt.Sprintf("%d", 45*1000),
	HTRACE_METRICS_MAX_ADDR_ENTRIES:    "100000",
	HTRACE_SPAN_EXPIRY_MS:              "0",
	HTRACE_REAPER_HEARTBEAT_PERIOD_MS:  fmt.Sprintf("%d", 90*1000),
}

// Values to be used when creating test configurations
func TEST_VALUES() map[string]string {
	return map[string]string{
		HTRACE_HRPC_ADDRESS: ":0",    // use a random port for the HRPC server
		HTRACE_LOG_LEVEL:    "TRACE", // show all log messages in tests
		HTRACE_WEB_ADDRESS:  ":0",    // use a random port for the REST server
		HTRACE_SPAN_EXPIRY_MS:"0",    // never time out spans (unless testing the reaper)
	}
}
