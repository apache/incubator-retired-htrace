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
	"encoding/gob"
	"github.com/jmhodges/levigo"
	"log"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
)

//
// The data store code for HTraced.
//
// This code stores the trace spans.  We use levelDB here so that we don't have to store everything
// in memory at all times.  The data is sharded across multiple levelDB databases in multiple
// directories.  Normally, these multiple directories will be on multiple disk drives.
//
// The main emphasis in the HTraceD data store is on quickly and efficiently storing trace span data
// coming from many daemons.  Durability is not as big a concern as in some data stores, since
// losing a little bit of trace data if htraced goes down is not critical.  We use the "gob" package
// for serialization.  We assume that there will be many more writes than reads.
//
// TODO: implement redundancy (storing data on more than 1 drive)
// TODO: implement re-loading old span data
//
// Schema
// m -> dataStoreMetadata
// s[8-byte-big-endian-sid] -> SpanData
// p[8-byte-big-endian-parent-sid][8-byte-big-endian-child-sid] -> {}
// t[8-byte-big-endian-time][8-byte-big-endian-child-sid] -> {}
//

const DATA_STORE_VERSION = 1

var EMPTY_BYTE_BUF []byte = []byte{}

type Statistics struct {
	NumSpansWritten uint64
}

func (stats *Statistics) IncrementWrittenSpans() {
	atomic.AddUint64(&stats.NumSpansWritten, 1)
}

// Make a copy of the statistics structure, using atomic operations.
func (stats *Statistics) Copy() *Statistics {
	return &Statistics{
		NumSpansWritten: atomic.LoadUint64(&stats.NumSpansWritten),
	}
}

// Translate a span id into a leveldb key.
func makeKey(tag byte, sid int64) []byte {
	id := uint64(sid)
	return []byte{
		tag,
		byte(0xff & (id >> 56)),
		byte(0xff & (id >> 48)),
		byte(0xff & (id >> 40)),
		byte(0xff & (id >> 32)),
		byte(0xff & (id >> 24)),
		byte(0xff & (id >> 16)),
		byte(0xff & (id >> 8)),
		byte(0xff & (id >> 0)),
	}
}

func keyToInt(key []byte) int64 {
	var id uint64
	id = (uint64(key[0]) << 56) |
		(uint64(key[1]) << 48) |
		(uint64(key[2]) << 40) |
		(uint64(key[3]) << 32) |
		(uint64(key[4]) << 24) |
		(uint64(key[5]) << 16) |
		(uint64(key[6]) << 8) |
		(uint64(key[7]) << 0)
	return int64(id)
}

func makeSecondaryKey(tag byte, first int64, second int64) []byte {
	fir := uint64(first)
	sec := uint64(second)
	return []byte{
		tag,
		byte(0xff & (fir >> 56)),
		byte(0xff & (fir >> 48)),
		byte(0xff & (fir >> 40)),
		byte(0xff & (fir >> 32)),
		byte(0xff & (fir >> 24)),
		byte(0xff & (fir >> 16)),
		byte(0xff & (fir >> 8)),
		byte(0xff & (fir >> 0)),
		byte(0xff & (sec >> 56)),
		byte(0xff & (sec >> 48)),
		byte(0xff & (sec >> 40)),
		byte(0xff & (sec >> 32)),
		byte(0xff & (sec >> 24)),
		byte(0xff & (sec >> 16)),
		byte(0xff & (sec >> 8)),
		byte(0xff & (sec >> 0)),
	}
}

// A single directory containing a levelDB instance.
type shard struct {
	// The data store that this shard is part of
	store *dataStore

	// The LevelDB instance.
	ldb *levigo.DB

	// The path to the leveldb directory this shard is managing.
	path string

	// Incoming requests to write Spans.
	incoming chan *common.Span

	// The channel we will send a bool to when we exit.
	exited chan bool
}

// Metadata about the DataStore.
type dataStoreMetadata struct {
	// The DataStore version.
	Version int32
}

// Write the metadata key to a shard.
func (shd *shard) WriteMetadata(meta *dataStoreMetadata) error {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(meta)
	if err != nil {
		return err
	}
	return shd.ldb.Put(shd.store.writeOpts, []byte("m"), w.Bytes())
}

// Process incoming spans for a shard.
func (shd *shard) processIncoming() {
	for {
		span := <-shd.incoming
		if span == nil {
			log.Printf("Shard processor for %s exiting.", shd.path)
			shd.exited <- true
			return
		}
		err := shd.writeSpan(span)
		if err != nil {
			log.Fatal("Shard processor for %s got fatal error %s.", shd.path, err.Error())
		}
		//log.Printf("Shard processor for %s wrote span %s.", shd.path, span.ToJson())
	}
}

func (shd *shard) writeSpan(span *common.Span) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()

	// Add SpanData to batch.
	spanDataBuf := new(bytes.Buffer)
	spanDataEnc := gob.NewEncoder(spanDataBuf)
	err := spanDataEnc.Encode(span.SpanData)
	if err != nil {
		return err
	}
	batch.Put(makeKey('s', span.Id.Val()), spanDataBuf.Bytes())

	// Add this to the parent index.
	for parentIdx := range span.Parents {
		batch.Put(makeSecondaryKey('p', span.Parents[parentIdx].Val(), span.Id.Val()), EMPTY_BYTE_BUF)
	}

	// Add this to the timeline index.
	batch.Put(makeSecondaryKey('t', span.Begin, span.Id.Val()), EMPTY_BYTE_BUF)

	err = shd.ldb.Write(shd.store.writeOpts, batch)
	if err != nil {
		return err
	}
	shd.store.stats.IncrementWrittenSpans()
	if shd.store.WrittenSpans != nil {
		shd.store.WrittenSpans <- span
	}
	return nil
}

func (shd *shard) FindChildren(sid int64, childIds []int64, lim int32) ([]int64, int32, error) {
	searchKey := makeKey('p', sid)
	iter := shd.ldb.NewIterator(shd.store.readOpts)
	defer iter.Close()
	iter.Seek(searchKey)
	for {
		if !iter.Valid() {
			break
		}
		if lim == 0 {
			break
		}
		key := iter.Key()
		if !bytes.HasPrefix(key, searchKey) {
			break
		}
		id := keyToInt(key[9:])
		childIds = append(childIds, id)
		lim--
		iter.Next()
	}
	return childIds, lim, nil
}

// Close a shard.
func (shd *shard) Close() {
	shd.incoming <- nil
	log.Printf("Waiting for %s to exit...", shd.path)
	if shd.exited != nil {
		<-shd.exited
	}
	shd.ldb.Close()
	log.Printf("Closed %s...", shd.path)
}

// The Data Store.
type dataStore struct {
	// The shards which manage our LevelDB instances.
	shards []*shard

	// I/O statistics for all shards.
	stats Statistics

	// The read options to use for LevelDB.
	readOpts *levigo.ReadOptions

	// The write options to use for LevelDB.
	writeOpts *levigo.WriteOptions

	// If non-null, a channel we will send spans to once we finish writing them.  This is only used
	// for testing.
	WrittenSpans chan *common.Span
}

func CreateDataStore(cnf *conf.Config, writtenSpans chan *common.Span) (*dataStore, error) {
	// Get the configuration.
	clearStored := cnf.GetBool(conf.HTRACE_DATA_STORE_CLEAR)
	dirsStr := cnf.Get(conf.HTRACE_DATA_STORE_DIRECTORIES)
	dirs := strings.Split(dirsStr, conf.PATH_LIST_SEP)

	// If we return an error, close the store.
	var err error
	store := &dataStore{shards: []*shard{}, WrittenSpans: writtenSpans}
	defer func() {
		if err != nil {
			store.Close()
			store = nil
		}
	}()

	store.readOpts = levigo.NewReadOptions()
	store.readOpts.SetFillCache(true)
	store.writeOpts = levigo.NewWriteOptions()
	store.writeOpts.SetSync(false)

	// Open all shards
	for idx := range dirs {
		path := dirs[idx] + conf.PATH_SEP + "db"
		err := os.MkdirAll(path, 0777)
		if err != nil {
			e, ok := err.(*os.PathError)
			if !ok || e.Err != syscall.EEXIST {
				return nil, err
			}
			if !clearStored {
				// TODO: implement re-opening saved data
				log.Println("Error: path " + path + "already exists.")
				return nil, err
			} else {
				err = os.RemoveAll(path)
				if err != nil {
					log.Println("Failed to create " + path + ": " + err.Error())
					return nil, err
				}
				log.Println("Cleared " + path)
			}
		}
		var shd *shard
		shd, err = CreateShard(store, cnf, path)
		if err != nil {
			log.Printf("Error creating shard %s: %s", path, err.Error())
			return nil, err
		}
		store.shards = append(store.shards, shd)
	}
	meta := &dataStoreMetadata{Version: DATA_STORE_VERSION}
	for idx := range store.shards {
		shd := store.shards[idx]
		err := shd.WriteMetadata(meta)
		if err != nil {
			log.Println("Failed to write metadata to " + store.shards[idx].path + ": " + err.Error())
			return nil, err
		}
		shd.exited = make(chan bool, 1)
		go shd.processIncoming()
	}
	return store, nil
}

func CreateShard(store *dataStore, cnf *conf.Config, path string) (*shard, error) {
	var shd *shard
	//filter := levigo.NewBloomFilter(10)
	//defer filter.Close()
	openOpts := levigo.NewOptions()
	defer openOpts.Close()
	openOpts.SetCreateIfMissing(true)
	//openOpts.SetFilterPolicy(filter)
	ldb, err := levigo.Open(path, openOpts)
	if err != nil {
		log.Println("LevelDB failed to open " + path + ": " + err.Error())
		return nil, err
	}
	defer func() {
		if shd == nil {
			ldb.Close()
		}
	}()
	spanBufferSize := cnf.GetInt(conf.HTRACE_DATA_STORE_SPAN_BUFFER_SIZE)
	shd = &shard{store: store, ldb: ldb, path: path,
		incoming: make(chan *common.Span, spanBufferSize)}
	log.Println("LevelDB opened " + path)
	return shd, nil
}

func (store *dataStore) GetStatistics() *Statistics {
	return store.stats.Copy()
}

// Close the DataStore.
func (store *dataStore) Close() {
	for idx := range store.shards {
		store.shards[idx].Close()
	}
	if store.readOpts != nil {
		store.readOpts.Close()
	}
	if store.writeOpts != nil {
		store.writeOpts.Close()
	}
}

// Get the index of the shard which stores the given spanId.
func (store *dataStore) getShardIndex(spanId int64) int {
	return int(uint64(spanId) % uint64(len(store.shards)))
}

func (store *dataStore) WriteSpan(span *common.Span) {
	store.shards[store.getShardIndex(span.Id.Val())].incoming <- span
}

func (store *dataStore) FindSpan(sid int64) *common.Span {
	return store.shards[store.getShardIndex(sid)].FindSpan(sid)
}

func (shd *shard) FindSpan(sid int64) *common.Span {
	buf, err := shd.ldb.Get(shd.store.readOpts, makeKey('s', sid))
	if err != nil {
		if strings.Index(err.Error(), "NotFound:") != -1 {
			return nil
		}
		log.Printf("Shard(%s): FindSpan(%d) error: %s\n",
			shd.path, sid, err.Error())
		return nil
	}
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	data := common.SpanData{}
	err = decoder.Decode(&data)
	if err != nil {
		log.Printf("Shard(%s): FindSpan(%d) decode error: %s\n",
			shd.path, sid, err.Error())
		return nil
	}
	// Gob encoding translates empty slices to nil.  Reverse this so that we're always dealing with
	// non-nil slices.
	if data.Parents == nil {
		data.Parents = []common.SpanId{}
	}
	return &common.Span{Id: common.SpanId(sid), SpanData: data}
}

// Find the children of a given span id.
func (store *dataStore) FindChildren(sid int64, lim int32) []int64 {
	childIds := make([]int64, 0)
	var err error

	startIdx := store.getShardIndex(sid)
	idx := startIdx
	numShards := len(store.shards)
	for {
		if lim == 0 {
			break
		}
		shd := store.shards[idx]
		childIds, lim, err = shd.FindChildren(sid, childIds, lim)
		if err != nil {
			log.Printf("Shard(%s): FindChildren(%d) error: %s\n",
				shd.path, sid, err.Error())
		}
		idx++
		if idx >= numShards {
			idx = 0
		}
		if idx == startIdx {
			break
		}
	}
	return childIds
}

//func (store *dataStore) FindByTimeRange(startTime int64, endTime int64, lim int32) []int64 {
//}
