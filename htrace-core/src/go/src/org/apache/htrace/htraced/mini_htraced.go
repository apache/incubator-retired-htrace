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
	"io/ioutil"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"strings"
)

//
// MiniHTraceD is used in unit tests to set up a daemon with certain settings.
// It takes care of things like creating and cleaning up temporary directories.
//

// The default number of managed data directories to use.
const DEFAULT_NUM_DATA_DIRS = 2

// Builds a MiniHTraced object.
type MiniHTracedBuilder struct {
	// The name of the MiniHTraced to build.  This shows up in the test directory name and some
	// other places.
	Name string

	// The configuration values to use for the MiniHTraced.
	// If ths is nil, we use the default configuration for everything.
	Cnf map[string]string

	// The number of managed data directories to create.
	// If this is 0, it defaults to DEFAULT_NUM_DATA_DIRS.
	NumDataDirs int

	// If non-null, the WrittenSpans channel to use when creating the DataStore.
	WrittenSpans chan *common.Span
}

type MiniHTraced struct {
	Name     string
	Cnf      *conf.Config
	DataDirs []string
	Store    *dataStore
	Rsv      *RestServer
}

func (bld *MiniHTracedBuilder) Build() (*MiniHTraced, error) {
	var err error
	var store *dataStore
	var rsv *RestServer
	if bld.Name == "" {
		bld.Name = "HTraceTest"
	}
	if bld.Cnf == nil {
		bld.Cnf = make(map[string]string)
	}
	if bld.NumDataDirs == 0 {
		bld.NumDataDirs = DEFAULT_NUM_DATA_DIRS
	}
	dataDirs := make([]string, bld.NumDataDirs)
	defer func() {
		if err != nil {
			if store != nil {
				store.Close()
			}
			for idx := range dataDirs {
				if dataDirs[idx] != "" {
					os.RemoveAll(dataDirs[idx])
				}
			}
			if rsv != nil {
				rsv.Close()
			}
		}
	}()
	for idx := range dataDirs {
		dataDirs[idx], err = ioutil.TempDir(os.TempDir(),
			fmt.Sprintf("%s%d", bld.Name, idx+1))
		if err != nil {
			return nil, err
		}
	}
	bld.Cnf[conf.HTRACE_DATA_STORE_DIRECTORIES] = strings.Join(dataDirs, conf.PATH_LIST_SEP)
	bld.Cnf[conf.HTRACE_WEB_ADDRESS] = ":0" // use a random port for the REST server
	cnfBld := conf.Builder{Values: bld.Cnf, Defaults: conf.DEFAULTS}
	cnf, err := cnfBld.Build()
	if err != nil {
		return nil, err
	}
	store, err = CreateDataStore(cnf, bld.WrittenSpans)
	if err != nil {
		return nil, err
	}
	rsv, err = CreateRestServer(cnf, store)
	if err != nil {
		return nil, err
	}
	return &MiniHTraced{
		Cnf:      cnf,
		DataDirs: dataDirs,
		Store:    store,
		Rsv:      rsv,
	}, nil
}

// Return a Config object that clients can use to connect to this MiniHTraceD.
func (ht *MiniHTraced) ClientConf() *conf.Config {
	return ht.Cnf.Clone(conf.HTRACE_WEB_ADDRESS, ht.Rsv.Addr().String())
}

func (ht *MiniHTraced) Close() {
	ht.Rsv.Close()
	ht.Store.Close()
	for idx := range ht.DataDirs {
		os.RemoveAll(ht.DataDirs[idx])
	}
}
