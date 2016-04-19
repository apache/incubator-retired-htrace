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
	"htrace/common"
	"htrace/conf"
	"io/ioutil"
	"net"
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

	// The DataDirs to use.  Empty entries will turn into random names.
	DataDirs []string

	// If true, we will keep the data dirs around after MiniHTraced#Close
	KeepDataDirsOnClose bool

	// If non-null, the WrittenSpans semaphore to use when creating the DataStore.
	WrittenSpans *common.Semaphore

	// The test hooks to use for the HRPC server
	HrpcTestHooks *hrpcTestHooks
}

type MiniHTraced struct {
	Name                string
	Cnf                 *conf.Config
	DataDirs            []string
	Store               *dataStore
	Rsv                 *RestServer
	Hsv                 *HrpcServer
	Lg                  *common.Logger
	KeepDataDirsOnClose bool
}

func (bld *MiniHTracedBuilder) Build() (*MiniHTraced, error) {
	var err error
	var store *dataStore
	var rsv *RestServer
	var hsv *HrpcServer
	if bld.Name == "" {
		bld.Name = "HTraceTest"
	}
	if bld.Cnf == nil {
		bld.Cnf = make(map[string]string)
	}
	if bld.DataDirs == nil {
		bld.DataDirs = make([]string, 2)
	}
	for idx := range bld.DataDirs {
		if bld.DataDirs[idx] == "" {
			bld.DataDirs[idx], err = ioutil.TempDir(os.TempDir(),
				fmt.Sprintf("%s%d", bld.Name, idx+1))
			if err != nil {
				return nil, err
			}
		}
	}
	// Copy the default test configuration values.
	for k, v := range conf.TEST_VALUES() {
		_, hasVal := bld.Cnf[k]
		if !hasVal {
			bld.Cnf[k] = v
		}
	}
	bld.Cnf[conf.HTRACE_DATA_STORE_DIRECTORIES] =
		strings.Join(bld.DataDirs, conf.PATH_LIST_SEP)
	cnfBld := conf.Builder{Values: bld.Cnf, Defaults: conf.DEFAULTS}
	cnf, err := cnfBld.Build()
	if err != nil {
		return nil, err
	}
	lg := common.NewLogger("mini.htraced", cnf)
	defer func() {
		if err != nil {
			if store != nil {
				store.Close()
			}
			for idx := range bld.DataDirs {
				if !bld.KeepDataDirsOnClose {
					if bld.DataDirs[idx] != "" {
						os.RemoveAll(bld.DataDirs[idx])
					}
				}
			}
			if rsv != nil {
				rsv.Close()
			}
			lg.Infof("Failed to create MiniHTraced %s: %s\n", bld.Name, err.Error())
			lg.Close()
		}
	}()
	store, err = CreateDataStore(cnf, bld.WrittenSpans)
	if err != nil {
		return nil, err
	}
	rstListener, listenErr := net.Listen("tcp", cnf.Get(conf.HTRACE_WEB_ADDRESS))
	if listenErr != nil {
		return nil, listenErr
	}
	defer func() {
		if rstListener != nil {
			rstListener.Close()
		}
	}()
	rsv, err = CreateRestServer(cnf, store, rstListener)
	if err != nil {
		return nil, err
	}
	rstListener = nil
	hsv, err = CreateHrpcServer(cnf, store, bld.HrpcTestHooks)
	if err != nil {
		return nil, err
	}

	lg.Infof("Created MiniHTraced %s\n", bld.Name)
	return &MiniHTraced{
		Name:                bld.Name,
		Cnf:                 cnf,
		DataDirs:            bld.DataDirs,
		Store:               store,
		Rsv:                 rsv,
		Hsv:                 hsv,
		Lg:                  lg,
		KeepDataDirsOnClose: bld.KeepDataDirsOnClose,
	}, nil
}

// Return a Config object that clients can use to connect to this MiniHTraceD.
func (ht *MiniHTraced) ClientConf() *conf.Config {
	return ht.Cnf.Clone(conf.HTRACE_WEB_ADDRESS, ht.Rsv.Addr().String(),
		conf.HTRACE_HRPC_ADDRESS, ht.Hsv.Addr().String())
}

// Return a Config object that clients can use to connect to this MiniHTraceD
// by HTTP only (no HRPC).
func (ht *MiniHTraced) RestOnlyClientConf() *conf.Config {
	return ht.Cnf.Clone(conf.HTRACE_WEB_ADDRESS, ht.Rsv.Addr().String(),
		conf.HTRACE_HRPC_ADDRESS, "")
}

func (ht *MiniHTraced) Close() {
	ht.Lg.Infof("Closing MiniHTraced %s\n", ht.Name)
	ht.Rsv.Close()
	ht.Hsv.Close()
	ht.Store.Close()
	if !ht.KeepDataDirsOnClose {
		for idx := range ht.DataDirs {
			ht.Lg.Infof("Removing %s...\n", ht.DataDirs[idx])
			os.RemoveAll(ht.DataDirs[idx])
		}
	}
	ht.Lg.Infof("Finished closing MiniHTraced %s\n", ht.Name)
	ht.Lg.Close()
}
