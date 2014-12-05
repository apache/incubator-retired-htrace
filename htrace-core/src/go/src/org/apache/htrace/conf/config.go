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
	"fmt"
	"io"
	"log"
	"os"
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

// Load a configuration from the application's argv, configuration file, and the standard
// defaults.
func LoadApplicationConfig() *Config {
	reader, err := openFile(CONFIG_FILE_NAME, []string{"."})
	if err != nil {
		log.Fatal("Error opening config file: " + err.Error())
	}
	bld := Builder{}
	if reader != nil {
		defer reader.Close()
		bld.Reader = bufio.NewReader(reader)
	}
	bld.Argv = os.Args[1:]
	bld.Defaults = DEFAULTS
	var cnf *Config
	cnf, err = bld.Build()
	if err != nil {
		log.Fatal("Error building configuration: " + err.Error())
	}
	os.Args = append(os.Args[0:1], bld.Argv...)
	return cnf
}

// Attempt to open a configuration file somewhere on the provided list of paths.
func openFile(cnfName string, paths []string) (io.ReadCloser, error) {
	for p := range paths {
		path := fmt.Sprintf("%s%c%s", paths[p], os.PathSeparator, cnfName)
		file, err := os.Open(path)
		if err == nil {
			log.Println("Reading configuration from " + path)
			return file, nil
		}
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			continue
		}
		log.Println("Error opening " + path + " for read: " + err.Error())
	}
	return nil, nil
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
		if strings.HasPrefix(str, "-D") {
			idx := strings.Index(str, "=")
			if idx == -1 {
				key := str[2:]
				cnf.settings[key] = "true"
			} else {
				key := str[2:idx]
				val := str[idx+1:]
				cnf.settings[key] = val
			}
			bld.Argv = append(bld.Argv[:i], bld.Argv[i+1:]...)
		} else {
			i++
		}
	}
	return &cnf, nil
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
