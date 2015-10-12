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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

//
// The configuration code for HTraced.
//
// HTraced can be configured via Hadoop-style XML configuration files, or by passing -Dkey=value
// command line arguments.  Command-line arguments without an equals sign, such as "-Dkey", will be
// treated as setting the key to "true".
//
// Configuration key constants should be defined in config_keys.go.  Each key should have a default,
// which will be used if the user supplies no value, or supplies an invalid value.
// For that reason, it is not necessary for the Get, GetInt, etc. functions to take a default value
// argument.
//
// Configuration objects are immutable.  However, you can make a copy of a configuration which adds
// some changes using Configuration#Clone().
//

type Config struct {
	settings map[string]string
	defaults map[string]string
}

type Builder struct {
	// If non-nil, the XML configuration file to read.
	Reader io.Reader

	// If non-nil, the configuration values to use.
	Values map[string]string

	// If non-nil, the default configuration values to use.
	Defaults map[string]string

	// If non-nil, the command-line arguments to use.
	Argv []string
}

func getDefaultHTracedConfDir() string {
	return PATH_SEP + "etc" + PATH_SEP + "htraced" + PATH_SEP + "conf"
}

func getHTracedConfDirs(dlog io.Writer) []string {
	confDir := os.Getenv("HTRACED_CONF_DIR")
	paths := filepath.SplitList(confDir)
	if len(paths) < 1 {
		def := getDefaultHTracedConfDir()
		io.WriteString(dlog, fmt.Sprintf("HTRACED_CONF_DIR defaulting to %s\n", def))
		return []string{def}
	}
	io.WriteString(dlog, fmt.Sprintf("HTRACED_CONF_DIR=%s\n", confDir))
	return paths
}

// Load a configuration from the application's argv, configuration file, and the standard
// defaults.
func LoadApplicationConfig() (*Config, io.Reader) {
	dlog := new(bytes.Buffer)
	reader := openFile(CONFIG_FILE_NAME, getHTracedConfDirs(dlog), dlog)
	bld := Builder{}
	if reader != nil {
		defer reader.Close()
		bld.Reader = bufio.NewReader(reader)
	}
	bld.Argv = os.Args[1:]
	bld.Defaults = DEFAULTS
	cnf, err := bld.Build()
	if err != nil {
		log.Fatal("Error building configuration: " + err.Error())
	}
	os.Args = append(os.Args[0:1], bld.Argv...)
	keys := make(sort.StringSlice, 0, 20)
	for k, _ := range cnf.settings {
		keys = append(keys, k)
	}
	sort.Sort(keys)
	for i := range keys {
		io.WriteString(dlog, fmt.Sprintf("%s = %s\n",
			keys[i], cnf.settings[keys[i]]))
	}
	return cnf, dlog
}

// Attempt to open a configuration file somewhere on the provided list of paths.
func openFile(cnfName string, paths []string, dlog io.Writer) io.ReadCloser {
	for p := range paths {
		path := fmt.Sprintf("%s%c%s", paths[p], os.PathSeparator, cnfName)
		file, err := os.Open(path)
		if err == nil {
			io.WriteString(dlog, fmt.Sprintf("Reading configuration from %s.\n", path))
			return file
		}
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			continue
		}
		io.WriteString(dlog, fmt.Sprintf("Error opening %s for read: %s\n", path, err.Error()))
	}
	return nil
}

// Try to parse a command-line element as a key=value pair.
func parseAsConfigFlag(flag string) (string, string) {
	var confPart string
	if strings.HasPrefix(flag, "-D") {
		confPart = flag[2:]
	} else if strings.HasPrefix(flag, "--D") {
		confPart = flag[3:]
	} else {
		return "", ""
	}
	if len(confPart) == 0 {
		return "", ""
	}
	idx := strings.Index(confPart, "=")
	if idx == -1 {
		return confPart, "true"
	}
	return confPart[0:idx], confPart[idx+1:]
}

// Build a new configuration object from the provided conf.Builder.
func (bld *Builder) Build() (*Config, error) {
	// Load values and defaults
	cnf := Config{}
	cnf.settings = make(map[string]string)
	if bld.Values != nil {
		for k, v := range bld.Values {
			cnf.settings[k] = v
		}
	}
	cnf.defaults = make(map[string]string)
	if bld.Defaults != nil {
		for k, v := range bld.Defaults {
			cnf.defaults[k] = v
		}
	}

	// Process the configuration file, if we have one
	if bld.Reader != nil {
		parseXml(bld.Reader, cnf.settings)
	}

	// Process command line arguments
	var i int
	for i < len(bld.Argv) {
		str := bld.Argv[i]
		key, val := parseAsConfigFlag(str)
		if key != "" {
			cnf.settings[key] = val
			bld.Argv = append(bld.Argv[:i], bld.Argv[i+1:]...)
		} else {
			i++
		}
	}
	return &cnf, nil
}

// Returns true if the configuration has a non-default value for the given key.
func (cnf *Config) Contains(key string) bool {
	_, ok := cnf.settings[key]
	return ok
}

// Get a string configuration key.
func (cnf *Config) Get(key string) string {
	ret := cnf.settings[key]
	if ret != "" {
		return ret
	}
	return cnf.defaults[key]
}

// Get a boolean configuration key.
func (cnf *Config) GetBool(key string) bool {
	str := cnf.settings[key]
	ret, err := strconv.ParseBool(str)
	if err == nil {
		return ret
	}
	str = cnf.defaults[key]
	ret, err = strconv.ParseBool(str)
	if err == nil {
		return ret
	}
	return false
}

// Get an integer configuration key.
func (cnf *Config) GetInt(key string) int {
	str := cnf.settings[key]
	ret, err := strconv.Atoi(str)
	if err == nil {
		return ret
	}
	str = cnf.defaults[key]
	ret, err = strconv.Atoi(str)
	if err == nil {
		return ret
	}
	return 0
}

// Get an int64 configuration key.
func (cnf *Config) GetInt64(key string) int64 {
	str := cnf.settings[key]
	ret, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return ret
	}
	str = cnf.defaults[key]
	ret, err = strconv.ParseInt(str, 10, 64)
	if err == nil {
		return ret
	}
	return 0
}

// Make a deep copy of the given configuration.
// Optionally, you can specify particular key/value pairs to change.
// Example:
// cnf2 := cnf.Copy("my.changed.key", "my.new.value")
func (cnf *Config) Clone(args ...string) *Config {
	if len(args)%2 != 0 {
		panic("The arguments to Config#copy are key1, value1, " +
			"key2, value2, and so on.  You must specify an even number of arguments.")
	}
	ncnf := &Config{defaults: cnf.defaults}
	ncnf.settings = make(map[string]string)
	for k, v := range cnf.settings {
		ncnf.settings[k] = v
	}
	for i := 0; i < len(args); i += 2 {
		ncnf.settings[args[i]] = args[i+1]
	}
	return ncnf
}
