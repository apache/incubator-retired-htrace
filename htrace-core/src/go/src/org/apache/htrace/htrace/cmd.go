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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v1"
	"io"
	"io/ioutil"
	"net/http"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
)

func main() {
	// Load htraced configuration
	cnf := conf.LoadApplicationConfig()

	// Parse argv
	app := kingpin.New("htrace", "The HTrace tracing utility.")
	addr := app.Flag("addr", "Server address.").
		Default(cnf.Get(conf.HTRACE_WEB_ADDRESS)).TCP()
	version := app.Command("version", "Print the version of this program.")
	serverInfo := app.Command("serverInfo", "Print information retrieved from an htraced server.")
	findSpan := app.Command("findSpan", "Print information about a trace span with a given ID.")
	findSpanId := findSpan.Flag("id", "Span ID to find, as a signed decimal 64-bit "+
		"number").Required().Uint64()
	findChildren := app.Command("findChildren", "Print out the span IDs that are children of a given span ID.")
	parentSpanId := findChildren.Flag("id", "Span ID to print children for, as a signed decimal 64-bit "+
		"number").Required().Uint64()
	childLim := findChildren.Flag("lim", "Maximum number of child IDs to print.").Default("20").Int()
	writeSpans := app.Command("writeSpans", "Write spans to the server in JSON form.")
	spanJson := writeSpans.Flag("json", "The JSON span data to write to the server.").Required().String()

	// Handle operation
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case version.FullCommand():
		os.Exit(printVersion())
	case serverInfo.FullCommand():
		os.Exit(printServerInfo((*addr).String()))
	case findSpan.FullCommand():
		os.Exit(doFindSpan((*addr).String(), *findSpanId))
	case findChildren.FullCommand():
		os.Exit(doFindChildren((*addr).String(), *parentSpanId, *childLim))
	case writeSpans.FullCommand():
		os.Exit(doWriteSpans((*addr).String(), *spanJson))
	}

	app.UsageErrorf(os.Stderr, "You must supply a command to run.")
}

// Print the version of the htrace binary.
func printVersion() int {
	fmt.Printf("Running htrace command version %s.\n", common.RELEASE_VERSION)
	return 0
}

// Print information retrieved from an htraced server via /server/info
func printServerInfo(restAddr string) int {
	buf, err := makeGetRequest(restAddr, "server/info")
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return 1
	}
	var info common.ServerInfo
	err = json.Unmarshal(buf, &info)
	if err != nil {
		fmt.Printf("Error: error unmarshalling response body %s: %s\n",
			string(buf), err.Error())
		return 1
	}
	fmt.Printf("HTraced server version %s (%s)\n", info.ReleaseVersion, info.GitVersion)
	return 0
}

// Print information about a trace span.
func doFindSpan(restAddr string, sid uint64) int {
	buf, err := makeGetRequest(restAddr, fmt.Sprintf("span/%016x", sid))
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return 1
	}
	var span common.Span
	err = json.Unmarshal(buf, &span)
	if err != nil {
		fmt.Printf("Error: error unmarshalling response body %s: %s\n",
			string(buf), err.Error())
		return 1
	}
	pbuf, err := json.MarshalIndent(span, "", "  ")
	if err != nil {
		fmt.Println("Error: error pretty-printing span to JSON: %s", err.Error())
		return 1
	}
	fmt.Printf("%s\n", string(pbuf))
	return 0
}

func doWriteSpans(restAddr string, spanJson string) int {
	body := []byte(spanJson)
	_, err := makeRestRequest("POST", restAddr, "writeSpans", bytes.NewReader(body))
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return 1
	}
	return 0
}

// Find information about the children of a span.
func doFindChildren(restAddr string, sid uint64, lim int) int {
	buf, err := makeGetRequest(restAddr, fmt.Sprintf("span/%016x/children?lim=%d", sid, lim))
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return 1
	}
	var spanIds []common.SpanId
	err = json.Unmarshal(buf, &spanIds)
	if err != nil {
		fmt.Printf("Error: error unmarshalling response body %s: %s\n",
			string(buf), err.Error())
		return 1
	}
	pbuf, err := json.MarshalIndent(spanIds, "", "  ")
	if err != nil {
		fmt.Println("Error: error pretty-printing span IDs to JSON: %s", err.Error())
		return 1
	}
	fmt.Printf("%s\n", string(pbuf))
	return 0
}

func makeGetRequest(restAddr string, reqName string) ([]byte, error) {
	return makeRestRequest("GET", restAddr, reqName, nil)
}

// Print information retrieved from an htraced server via /serverInfo
func makeRestRequest(reqType string, restAddr string, reqName string,
	reqBody io.Reader) ([]byte, error) {
	url := fmt.Sprintf("http://%s/%s", restAddr, reqName)
	req, err := http.NewRequest(reqType, url, reqBody)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error: error making http request to %s: %s\n", url,
			err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("Error: got bad response status from %s: %s\n", url, resp.Status))
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error: error reading response body: %s\n", err.Error()))
	}
	return body, nil
}
