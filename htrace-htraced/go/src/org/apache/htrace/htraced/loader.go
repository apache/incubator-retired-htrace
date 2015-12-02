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
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/ugorji/go/codec"
	"io"
	"math"
	"math/rand"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"strings"
	"syscall"
	"time"
)

// Routines for loading the datastore.

// The leveldb key which has information about the shard.
const SHARD_INFO_KEY = 'w'

// A constant signifying that we don't know what the layout version is.
const UNKNOWN_LAYOUT_VERSION = 0

// The current layout version.  We cannot read layout versions newer than this.
// We may sometimes be able to read older versions, but only by doing an
// upgrade.
const CURRENT_LAYOUT_VERSION = 3

type DataStoreLoader struct {
	// The dataStore logger.
	lg *common.Logger

	// True if we should clear the stored data.
	ClearStored bool

	// The shards that we're loading
	shards []*ShardLoader

	// The options to use for opening datastores in LevelDB.
	openOpts *levigo.Options

	// The read options to use for LevelDB.
	readOpts *levigo.ReadOptions

	// The write options to use for LevelDB.
	writeOpts *levigo.WriteOptions
}

// Information about a Shard.
type ShardInfo struct {
	// The layout version of the datastore.
	// We should always keep this field so that old software can recognize new
	// layout versions, even if it can't read them.
	LayoutVersion uint64

	// A random number identifying this daemon.
	DaemonId uint64

	// The total number of shards in this datastore.
	TotalShards uint32

	// The index of this shard within the datastore.
	ShardIndex uint32
}

// Create a new datastore loader. 
// Initializes the loader, but does not load any leveldb instances.
func NewDataStoreLoader(cnf *conf.Config) *DataStoreLoader {
	dld := &DataStoreLoader{
		lg: common.NewLogger("datastore", cnf),
		ClearStored: cnf.GetBool(conf.HTRACE_DATA_STORE_CLEAR),
	}
	dld.readOpts = levigo.NewReadOptions()
	dld.readOpts.SetFillCache(true)
	dld.readOpts.SetVerifyChecksums(false)
	dld.writeOpts = levigo.NewWriteOptions()
	dld.writeOpts.SetSync(false)
	dirsStr := cnf.Get(conf.HTRACE_DATA_STORE_DIRECTORIES)
	rdirs := strings.Split(dirsStr, conf.PATH_LIST_SEP)
	// Filter out empty entries
	dirs := make([]string, 0, len(rdirs))
	for i := range(rdirs) {
		if strings.TrimSpace(rdirs[i]) != "" {
			dirs = append(dirs, rdirs[i])
		}
	}
	dld.shards = make([]*ShardLoader, len(dirs))
	for i := range(dirs) {
		dld.shards[i] = &ShardLoader{
			dld: dld,
			path: dirs[i] + conf.PATH_SEP + "db",
		}
	}
	dld.openOpts = levigo.NewOptions()
	dld.openOpts.SetParanoidChecks(false)
	writeBufferSize := cnf.GetInt(conf.HTRACE_LEVELDB_WRITE_BUFFER_SIZE)
	if writeBufferSize > 0 {
		dld.openOpts.SetWriteBufferSize(writeBufferSize)
	}
	maxFdPerShard := dld.calculateMaxOpenFilesPerShard()
	if maxFdPerShard > 0 {
		dld.openOpts.SetMaxOpenFiles(maxFdPerShard)
	}
	return dld
}

func (dld *DataStoreLoader) Close() {
	if dld.lg != nil {
		dld.lg.Close()
		dld.lg = nil
	}
	if dld.openOpts != nil {
		dld.openOpts.Close()
		dld.openOpts = nil
	}
	if dld.readOpts != nil {
		dld.readOpts.Close()
		dld.readOpts = nil
	}
	if dld.writeOpts != nil {
		dld.writeOpts.Close()
		dld.writeOpts = nil
	}
	if dld.shards != nil {
		for i := range(dld.shards) {
			if dld.shards[i] != nil {
				dld.shards[i].Close()
			}
		}
		dld.shards = nil
	}
}

func (dld *DataStoreLoader) DisownResources() {
	dld.lg = nil
	dld.openOpts = nil
	dld.readOpts = nil
	dld.writeOpts = nil
	dld.shards = nil
}

// The maximum number of file descriptors we'll use on non-datastore things.
const NON_DATASTORE_FD_MAX = 300

// The minimum number of file descriptors per shard we will set.  Setting fewer
// than this number could trigger a bug in some early versions of leveldb.
const MIN_FDS_PER_SHARD = 80

func (dld *DataStoreLoader) calculateMaxOpenFilesPerShard() int {
	var rlim syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
	if err != nil {
		dld.lg.Warnf("Unable to calculate maximum open files per shard: " +
			"getrlimit failed: %s\n", err.Error())
		return 0
	}
	// I think RLIMIT_NOFILE fits in 32 bits on all known operating systems,
	// but there's no harm in being careful.  'int' in golang always holds at
	// least 32 bits.
	var maxFd int
	if rlim.Cur > uint64(math.MaxInt32) {
		maxFd = math.MaxInt32
	} else {
		maxFd = int(rlim.Cur)
	}
	if len(dld.shards) == 0 {
		dld.lg.Warnf("Unable to calculate maximum open files per shard, " +
			"since there are 0 shards configured.\n")
		return 0
	}
	fdsPerShard := (maxFd - NON_DATASTORE_FD_MAX) / len(dld.shards)
	if fdsPerShard < MIN_FDS_PER_SHARD {
		dld.lg.Warnf("Expected to be able to use at least %d " +
			"fds per shard, but we have %d shards and %d total fds to allocate, " +
			"giving us only %d FDs per shard.", MIN_FDS_PER_SHARD,
			len(dld.shards), maxFd - NON_DATASTORE_FD_MAX, fdsPerShard)
		return 0
	}
	dld.lg.Infof("maxFd = %d.  Setting maxFdPerShard = %d\n",
		maxFd, fdsPerShard)
	return fdsPerShard
}

// Load information about all shards.
func (dld *DataStoreLoader) LoadShards() {
	for i := range(dld.shards) {
		shd := dld.shards[i]
		shd.load()
	}
}

// Verify that the shard infos are consistent.
// Reorders the shardInfo structures based on their ShardIndex.
func (dld *DataStoreLoader) VerifyShardInfos() error {
	if len(dld.shards) < 1 {
		return errors.New("No shard directories found.")
	}
	// Make sure no shards had errors.
	for i := range(dld.shards) {
		shd := dld.shards[i]
		if shd.infoErr != nil {
			return shd.infoErr
		}
	}
	// Make sure that if any shards are empty, all shards are empty.
	emptyShards := ""
	prefix := ""
	for i := range(dld.shards) {
		if dld.shards[i].info == nil {
			emptyShards = prefix + dld.shards[i].path
			prefix = ", "
		}
	}
	if emptyShards != "" {
		for i := range(dld.shards) {
			if dld.shards[i].info != nil {
				return errors.New(fmt.Sprintf("Shards %s were empty, but " +
					"the other shards had data.", emptyShards))
			}
		}
		// All shards are empty.
		return nil
	}
	// Make sure that all shards have the same layout version, daemonId, and number of total
	// shards.
	layoutVersion := dld.shards[0].info.LayoutVersion
	daemonId := dld.shards[0].info.DaemonId
	totalShards := dld.shards[0].info.TotalShards
	for i := 1; i < len(dld.shards); i++ {
		shd := dld.shards[i]
		if layoutVersion != shd.info.LayoutVersion {
			return errors.New(fmt.Sprintf("Layout version mismatch.  Shard " +
				"%s has layout version 0x%016x, but shard %s has layout " +
				"version 0x%016x.",
				dld.shards[0].path, layoutVersion, shd.path, shd.info.LayoutVersion))
		}
		if daemonId != shd.info.DaemonId {
			return errors.New(fmt.Sprintf("DaemonId mismatch. Shard %s has " +
			"daemonId 0x%016x, but shard %s has daemonId 0x%016x.",
				dld.shards[0].path, daemonId, shd.path, shd.info.DaemonId))
		}
		if totalShards != shd.info.TotalShards {
			return errors.New(fmt.Sprintf("TotalShards mismatch.  Shard %s has " +
				"TotalShards = %d, but shard %s has TotalShards = %d.",
				dld.shards[0].path, totalShards, shd.path, shd.info.TotalShards))
		}
		if shd.info.ShardIndex >= totalShards {
			return errors.New(fmt.Sprintf("Invalid ShardIndex.  Shard %s has " +
				"ShardIndex = %d, but TotalShards = %d.",
				shd.path, shd.info.ShardIndex, shd.info.TotalShards))
		}
	}
	if layoutVersion != CURRENT_LAYOUT_VERSION {
		return errors.New(fmt.Sprintf("The layout version of all shards " +
			"is %d, but we only support version %d.",
			layoutVersion, CURRENT_LAYOUT_VERSION))
	}
	if totalShards != uint32(len(dld.shards)) {
		return errors.New(fmt.Sprintf("The TotalShards field of all shards " +
			"is %d, but we have %d shards.", totalShards, len(dld.shards)))
	}
	// Reorder shards in order of their ShardIndex.
	reorderedShards := make([]*ShardLoader, len(dld.shards))
	for i := 0; i < len(dld.shards); i++ {
		shd := dld.shards[i]
		shardIdx := shd.info.ShardIndex
		if reorderedShards[shardIdx] != nil {
			return errors.New(fmt.Sprintf("Both shard %s and " +
				"shard %s have ShardIndex %d.", shd.path,
				reorderedShards[shardIdx].path, shardIdx))
		}
		reorderedShards[shardIdx] = shd
	}
	dld.shards = reorderedShards
	return nil
}

func (dld *DataStoreLoader) Load() error {
	var err error
	// If data.store.clear was set, clear existing data.
	if dld.ClearStored {
		err = dld.clearStored()
		if err != nil {
			return err
		}
	}
	// Make sure the shard directories exist in all cases, with a mkdir -p 
	for i := range dld.shards {
		err := os.MkdirAll(dld.shards[i].path, 0777)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to MkdirAll(%s): %s",
				dld.shards[i].path, err.Error()))
		}
	}
	// Get information about each shard, and verify them.
	dld.LoadShards()
	err = dld.VerifyShardInfos()
	if err != nil {
		return err
	}
	if dld.shards[0].ldb != nil {
		dld.lg.Infof("Loaded %d leveldb instances with " +
			"DaemonId of 0x%016x\n", len(dld.shards),
			dld.shards[0].info.DaemonId)
	} else {
		// Create leveldb instances if needed.
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		daemonId := uint64(rnd.Int63())
		dld.lg.Infof("Initializing %d leveldb instances with a new " +
			"DaemonId of 0x%016x\n", len(dld.shards), daemonId)
		dld.openOpts.SetCreateIfMissing(true)
		for i := range(dld.shards) {
			shd := dld.shards[i]
			shd.ldb, err = levigo.Open(shd.path, shd.dld.openOpts)
			if err != nil {
				return errors.New(fmt.Sprintf("levigo.Open(%s) failed to " +
					"create the shard: %s", shd.path, err.Error()))
			}
			info := &ShardInfo {
				LayoutVersion: CURRENT_LAYOUT_VERSION,
				DaemonId: daemonId,
				TotalShards: uint32(len(dld.shards)),
				ShardIndex: uint32(i),
			}
			err = shd.writeShardInfo(info)
			if err != nil {
				return errors.New(fmt.Sprintf("levigo.Open(%s) failed to " +
					"write shard info: %s", shd.path, err.Error()))
			}
			dld.lg.Infof("Shard %s initialized with ShardInfo %s \n",
				shd.path, asJson(info))
		}
	}
	return nil
}

func (dld *DataStoreLoader) clearStored() error {
	for i := range dld.shards {
		path := dld.shards[i].path
		fi, err := os.Stat(path)
		if err != nil && !os.IsNotExist(err) {
			dld.lg.Errorf("Failed to stat %s: %s\n", path, err.Error())
			return err
		}
		if fi != nil {
			err = os.RemoveAll(path)
			if err != nil {
				dld.lg.Errorf("Failed to clear existing datastore directory %s: %s\n",
					path, err.Error())
				return err
			}
			dld.lg.Infof("Cleared existing datastore directory %s\n", path)
		}
	}
	return nil
}

type ShardLoader struct {
	// The parent DataStoreLoader
	dld *DataStoreLoader

	// Path to the shard
	path string

	// Leveldb instance of the shard
	ldb *levigo.DB

	// Information about the shard
	info *ShardInfo

	// If non-null, the error we encountered trying to load the shard info.
	infoErr error
}

func (shd *ShardLoader) Close() {
	if shd.ldb != nil {
		shd.ldb.Close()
		shd.ldb = nil
	}
}

// Load information about a particular shard.
func (shd *ShardLoader) load() {
	shd.info = nil
	fi, err := os.Stat(shd.path)
	if err != nil {
		if os.IsNotExist(err) {
			shd.infoErr = nil
			return
		}
		shd.infoErr = errors.New(fmt.Sprintf(
			"stat() error on leveldb directory " +
				"%s: %s", shd.path, err.Error()))
		return
	}
	if !fi.Mode().IsDir() {
		shd.infoErr = errors.New(fmt.Sprintf(
			"stat() error on leveldb directory " +
				"%s: inode is not directory.", shd.path))
		return
	}
	var dbDir *os.File
	dbDir, err = os.Open(shd.path)
	if err != nil {
		shd.infoErr = errors.New(fmt.Sprintf(
			"open() error on leveldb directory " +
				"%s: %s.", shd.path, err.Error()))
		return
	}
	defer func() {
		if dbDir != nil {
			dbDir.Close()
		}
	}()
	_, err = dbDir.Readdirnames(1)
	if err != nil {
		if err == io.EOF {
			// The db directory is empty.
			shd.infoErr = nil
			return
		}
		shd.infoErr = errors.New(fmt.Sprintf(
			"Readdirnames() error on leveldb directory " +
				"%s: %s.", shd.path, err.Error()))
		return
	}
	dbDir.Close()
	dbDir = nil
	shd.ldb, err = levigo.Open(shd.path, shd.dld.openOpts)
	if err != nil {
		shd.ldb = nil
		shd.infoErr = errors.New(fmt.Sprintf(
			"levigo.Open() error on leveldb directory " +
				"%s: %s.", shd.path, err.Error()))
		return
	}
	shd.info, err = shd.readShardInfo()
	if err != nil {
		shd.infoErr = err
		return
	}
	shd.infoErr = nil
}

func (shd *ShardLoader) readShardInfo() (*ShardInfo, error) {
	buf, err := shd.ldb.Get(shd.dld.readOpts, []byte{SHARD_INFO_KEY})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("readShardInfo(%s): failed to " +
			"read shard info key: %s", shd.path, err.Error()))
	}
	if len(buf) == 0 {
		return nil, errors.New(fmt.Sprintf("readShardInfo(%s): got zero-" +
			"length value for shard info key.", shd.path))
	}
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	r := bytes.NewBuffer(buf)
	decoder := codec.NewDecoder(r, mh)
	shardInfo := &ShardInfo {
		LayoutVersion: UNKNOWN_LAYOUT_VERSION,
	}
	err = decoder.Decode(shardInfo)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("readShardInfo(%s): msgpack " +
			"decoding failed for shard info key: %s", shd.path, err.Error()))
	}
	return shardInfo, nil
}

func (shd *ShardLoader) writeShardInfo(info *ShardInfo) error {
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	w := new(bytes.Buffer)
	enc := codec.NewEncoder(w, mh)
	err := enc.Encode(info)
	if err != nil {
		return errors.New(fmt.Sprintf("msgpack encoding error: %s",
			err.Error()))
	}
	err = shd.ldb.Put(shd.dld.writeOpts, []byte{SHARD_INFO_KEY}, w.Bytes())
	if err != nil {
		return errors.New(fmt.Sprintf("leveldb write error: %s",
			err.Error()))
	}
	return nil
}
