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
	"errors"
	"fmt"
	"github.com/alecthomas/kingpin"
	"io"
	htrace "org/apache/htrace/client"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"time"
)

var RELEASE_VERSION string
var GIT_VERSION string

const EXIT_SUCCESS = 0
const EXIT_FAILURE = 1

var verbose *bool

const USAGE = `The Apache HTrace command-line tool.  This tool retrieves and modifies settings and
other data on a running htraced daemon.

If we find an ` + conf.CONFIG_FILE_NAME + ` configuration file in the list of directories
specified in ` + conf.HTRACED_CONF_DIR + `, we will use that configuration; otherwise, 
the defaults will be used.
`

func main() {
	// Load htraced configuration
	cnf := conf.LoadApplicationConfig()

	// Parse argv
	app := kingpin.New(os.Args[0], USAGE)
	app.Flag("Dmy.key", "Set configuration key 'my.key' to 'my.value'.  Replace 'my.key' "+
		"with any key you want to set.").Default("my.value").String()
	addr := app.Flag("addr", "Server address.").String()
	verbose = app.Flag("verbose", "Verbose.").Default("false").Bool()
	version := app.Command("version", "Print the version of this program.")
	serverInfo := app.Command("serverInfo", "Print information retrieved from an htraced server.")
	findSpan := app.Command("findSpan", "Print information about a trace span with a given ID.")
	findSpanId := findSpan.Arg("id", "Span ID to find. Example: 0x123456789abcdef").Required().Uint64()
	findChildren := app.Command("findChildren", "Print out the span IDs that are children of a given span ID.")
	parentSpanId := findChildren.Arg("id", "Span ID to print children for. Example: 0x123456789abcdef").
		Required().Uint64()
	childLim := findChildren.Flag("lim", "Maximum number of child IDs to print.").Default("20").Int()
	loadFile := app.Command("loadFile", "Write whitespace-separated JSON spans from a file to the server.")
	loadFilePath := loadFile.Arg("path",
		"A file containing whitespace-separated span JSON.").Required().String()
	dumpAll := app.Command("dumpAll", "Dump all spans from the htraced daemon.")
	dumpAllOutPath := dumpAll.Flag("path", "The path to dump the trace spans to.").Default("-").String()
	dumpAllLim := dumpAll.Flag("lim", "The number of spans to transfer from the server at once.").
		Default("100").Int()
	loadJson := app.Command("load", "Write JSON spans from the command-line to the server.")
	loadJsonArg := loadJson.Arg("json", "A JSON span to write to the server.").Required().String()
	graph := app.Command("graph", "Visualize span JSON as a graph.")
	graphJsonFile := graph.Flag("input", "The JSON file to load").Required().String()
	graphDotFile := graph.Flag("output",
		"The path to write a GraphViz dotfile to.  This file can be used as input to "+
			"GraphViz, in order to generate a pretty picture.  See graphviz.org for more "+
			"information about generating pictures of graphs.").Default("-").String()
	query := app.Command("query", "Send a query to htraced.")
	queryLim := query.Flag("lim", "Maximum number of spans to retrieve.").Default("20").Int()
	queryArg := query.Arg("query", "The query string to send.  Query strings have the format "+
		"[TYPE] [OPERATOR] [CONST], joined by AND statements.").Required().String()
	rawQuery := app.Command("rawQuery", "Send a raw JSON query to htraced.")
	rawQueryArg := query.Arg("json", "The query JSON to send.").Required().String()
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// Add the command-line settings into the configuration.
	if *addr != "" {
		cnf = cnf.Clone(conf.HTRACE_WEB_ADDRESS, *addr)
	}

	// Handle commands that don't require an HTrace client.
	switch cmd {
	case version.FullCommand():
		os.Exit(printVersion())
	case graph.FullCommand():
		err := jsonSpanFileToDotFile(*graphJsonFile, *graphDotFile)
		if err != nil {
			fmt.Printf("graphing error: %s\n", err.Error())
			os.Exit(EXIT_FAILURE)
		}
		os.Exit(EXIT_SUCCESS)
	}

	// Create HTrace client
	hcl, err := htrace.NewClient(cnf)
	if err != nil {
		fmt.Printf("Failed to create HTrace client: %s\n", err.Error())
		os.Exit(EXIT_FAILURE)
	}

	// Handle commands that require an HTrace client.
	switch cmd {
	case version.FullCommand():
		os.Exit(printVersion())
	case serverInfo.FullCommand():
		os.Exit(printServerInfo(hcl))
	case findSpan.FullCommand():
		os.Exit(doFindSpan(hcl, common.SpanId(*findSpanId)))
	case findChildren.FullCommand():
		os.Exit(doFindChildren(hcl, common.SpanId(*parentSpanId), *childLim))
	case loadJson.FullCommand():
		os.Exit(doLoadSpanJson(hcl, *loadJsonArg))
	case loadFile.FullCommand():
		os.Exit(doLoadSpanJsonFile(hcl, *loadFilePath))
	case dumpAll.FullCommand():
		err := doDumpAll(hcl, *dumpAllOutPath, *dumpAllLim)
		if err != nil {
			fmt.Printf("dumpAll error: %s\n", err.Error())
			os.Exit(EXIT_FAILURE)
		}
		os.Exit(EXIT_SUCCESS)
	case query.FullCommand():
		err := doQueryFromString(hcl, *queryArg, *queryLim)
		if err != nil {
			fmt.Printf("query error: %s\n", err.Error())
			os.Exit(EXIT_FAILURE)
		}
		os.Exit(EXIT_SUCCESS)
	case rawQuery.FullCommand():
		err := doRawQuery(hcl, *rawQueryArg)
		if err != nil {
			fmt.Printf("raw query error: %s\n", err.Error())
			os.Exit(EXIT_FAILURE)
		}
		os.Exit(EXIT_SUCCESS)
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

func doLoadSpanJsonFile(hcl *htrace.Client, spanFile string) int {
	if spanFile == "" {
		fmt.Printf("You must specify the json file to load.\n")
		return EXIT_FAILURE
	}
	file, err := OpenInputFile(spanFile)
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
			fmt.Printf("Failed to decode JSON: %s\n", err.Error())
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

func doLoadSpanJson(hcl *htrace.Client, spanJson string) int {
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

// Dump all spans from the htraced daemon.
func doDumpAll(hcl *htrace.Client, outPath string, lim int) error {
	file, err := CreateOutputFile(outPath)
	if err != nil {
		return err
	}
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	out := make(chan *common.Span, 50)
	var dumpErr error
	go func() {
		dumpErr = hcl.DumpAll(lim, out)
	}()
	var numSpans int64
	nextLogTime := time.Now().Add(time.Second * 5)
	for {
		span, channelOpen := <-out
		if !channelOpen {
			break
		}
		if err != nil {
			_, err = fmt.Fprintf(file, "%s\n", span.ToJson())
		}
		if *verbose {
			numSpans++
			now := time.Now()
			if !now.Before(nextLogTime) {
				nextLogTime = now.Add(time.Second * 5)
				fmt.Printf("wrote %d span(s)...\n", numSpans)
			}
		}
	}
	if err != nil {
		return errors.New(fmt.Sprintf("Write error %s", err.Error()))
	}
	if dumpErr != nil {
		return errors.New(fmt.Sprintf("Dump error %s", dumpErr.Error()))
	}
	err = file.Close()
	file = nil
	if err != nil {
		return err
	}
	return nil
}
