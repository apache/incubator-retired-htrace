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
	"io"
	htrace "org/apache/htrace/client"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
)

var RELEASE_VERSION string
var GIT_VERSION string

const EXIT_SUCCESS = 0
const EXIT_FAILURE = 1

var verbose *bool

func main() {
	// Parse argv
	app := kingpin.New("htrace", "The HTrace tracing utility.")
	addr := app.Flag("addr", "Server address.").
		Default(conf.DEFAULTS[conf.HTRACE_WEB_ADDRESS]).TCP()
	verbose = app.Flag("verbose", "Verbose.").Default("false").Bool()
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
	spanJson := writeSpans.Flag("json", "The JSON span data to write to the server.").String()
	spanFile := writeSpans.Flag("file",
		"A file containing JSON span data to write to the server.").String()
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// Load htraced configuration
	values := make(map[string]string)
	values[conf.HTRACE_WEB_ADDRESS] = (*addr).String()
	cnf := conf.LoadApplicationConfig(values)

	// Create HTrace client
	hcl, err := htrace.NewClient(cnf)
	if err != nil {
		fmt.Printf("Failed to create HTrace client: %s\n", err.Error())
		os.Exit(EXIT_FAILURE)
	}

	// Handle operation
	switch cmd {
	case version.FullCommand():
		os.Exit(printVersion())
	case serverInfo.FullCommand():
		os.Exit(printServerInfo(hcl))
	case findSpan.FullCommand():
		os.Exit(doFindSpan(hcl, common.SpanId(*findSpanId)))
	case findChildren.FullCommand():
		os.Exit(doFindChildren(hcl, common.SpanId(*parentSpanId), *childLim))
	case writeSpans.FullCommand():
		if *spanJson != "" {
			if *spanFile != "" {
				fmt.Printf("You must specify either --json or --file, " +
					"but not both.\n")
				os.Exit(EXIT_FAILURE)
			}
			os.Exit(doWriteSpanJson(hcl, *spanJson))
		} else if *spanFile != "" {
			os.Exit(doWriteSpanJsonFile(hcl, *spanFile))
		}
		fmt.Printf("You must specify either --json or --file.\n")
		os.Exit(EXIT_FAILURE)
	}

	app.UsageErrorf(os.Stderr, "You must supply a command to run.")
}

// Print the version of the htrace binary.
func printVersion() int {
	fmt.Printf("Running htrace command version %s.\n", RELEASE_VERSION)
	return EXIT_SUCCESS
}

// Print information retrieved from an htraced server via /server/info
func printServerInfo(hcl *htrace.Client) int {
	info, err := hcl.GetServerInfo()
	if err != nil {
		fmt.Println(err.Error())
		return EXIT_FAILURE
	}
	fmt.Printf("HTraced server version %s (%s)\n", info.ReleaseVersion, info.GitVersion)
	return EXIT_SUCCESS
}

// Print information about a trace span.
func doFindSpan(hcl *htrace.Client, sid common.SpanId) int {
	span, err := hcl.FindSpan(sid)
	if err != nil {
		fmt.Println(err.Error())
		return EXIT_FAILURE
	}
	if span == nil {
		fmt.Printf("Span ID not found.\n")
		return EXIT_FAILURE
	}
	pbuf, err := json.MarshalIndent(span, "", "  ")
	if err != nil {
		fmt.Printf("Error: error pretty-printing span to JSON: %s\n", err.Error())
		return EXIT_FAILURE
	}
	fmt.Printf("%s\n", string(pbuf))
	return EXIT_SUCCESS
}

func doWriteSpanJsonFile(hcl *htrace.Client, spanFile string) int {
	file, err := os.Open(spanFile)
	if err != nil {
		fmt.Printf("Failed to open %s: %s\n", spanFile, err.Error())
		return EXIT_FAILURE
	}
	defer file.Close()
	in := bufio.NewReader(file)
	dec := json.NewDecoder(in)
	for {
		var span common.Span
		if err = dec.Decode(&span); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Failed to decode JSON: %s", err.Error())
			return EXIT_FAILURE
		}
		if *verbose {
			fmt.Printf("wrote %s\n", span.ToJson())
		}
		if err = hcl.WriteSpan(&span); err != nil {
			fmt.Println(err.Error())
			return EXIT_FAILURE
		}
	}
	return EXIT_SUCCESS
}

func doWriteSpanJson(hcl *htrace.Client, spanJson string) int {
	spanBytes := []byte(spanJson)
	var span common.Span
	err := json.Unmarshal(spanBytes, &span)
	if err != nil {
		fmt.Printf("Error parsing provided JSON: %s\n", err.Error())
		return EXIT_FAILURE
	}
	err = hcl.WriteSpan(&span)
	if err != nil {
		fmt.Println(err.Error())
		return EXIT_FAILURE
	}
	return EXIT_SUCCESS
}

// Find information about the children of a span.
func doFindChildren(hcl *htrace.Client, sid common.SpanId, lim int) int {
	spanIds, err := hcl.FindChildren(sid, lim)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return EXIT_FAILURE
	}
	pbuf, err := json.MarshalIndent(spanIds, "", "  ")
	if err != nil {
		fmt.Println("Error: error pretty-printing span IDs to JSON: %s", err.Error())
		return 1
	}
	fmt.Printf("%s\n", string(pbuf))
	return 0
}
