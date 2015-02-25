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
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"strconv"
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
// Schema
// m -> dataStoreMetadata
// s[8-byte-big-endian-sid] -> SpanData
// b[8-byte-big-endian-begin-time][8-byte-big-endian-child-sid] -> {}
// e[8-byte-big-endian-end-time][8-byte-big-endian-child-sid] -> {}
// d[8-byte-big-endian-duration][8-byte-big-endian-child-sid] -> {}
// p[8-byte-big-endian-parent-sid][8-byte-big-endian-child-sid] -> {}
//
// Note that span IDs are unsigned 64-bit numbers.
// Begin times, end times, and durations are signed 64-bit numbers.
// In order to get LevelDB to properly compare the signed 64-bit quantities,
// we flip the highest bit.  This way, we can get leveldb to view negative
// quantities as less than non-negative ones.  This also means that we can do
// all queries using unsigned 64-bit math, rather than having to special-case
// the signed fields.
//

const DATA_STORE_VERSION = 1

var EMPTY_BYTE_BUF []byte = []byte{}

const SPAN_ID_INDEX_PREFIX = 's'
const BEGIN_TIME_INDEX_PREFIX = 'b'
const END_TIME_INDEX_PREFIX = 'e'
const DURATION_INDEX_PREFIX = 'd'
const PARENT_ID_INDEX_PREFIX = 'p'
const INVALID_INDEX_PREFIX = 0

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
func makeKey(tag byte, sid uint64) []byte {
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

func keyToInt(key []byte) uint64 {
	var id uint64
	id = (uint64(key[0]) << 56) |
		(uint64(key[1]) << 48) |
		(uint64(key[2]) << 40) |
		(uint64(key[3]) << 32) |
		(uint64(key[4]) << 24) |
		(uint64(key[5]) << 16) |
		(uint64(key[6]) << 8) |
		(uint64(key[7]) << 0)
	return id
}

func makeSecondaryKey(tag byte, fir uint64, sec uint64) []byte {
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
	lg := shd.store.lg
	for {
		span := <-shd.incoming
		if span == nil {
			lg.Infof("Shard processor for %s exiting.\n", shd.path)
			shd.exited <- true
			return
		}
		err := shd.writeSpan(span)
		if err != nil {
			lg.Errorf("Shard processor for %s got fatal error %s.\n", shd.path, err.Error())
		} else {
			lg.Tracef("Shard processor for %s wrote span %s.\n", shd.path, span.ToJson())
		}
	}
}

// Convert a signed 64-bit number into an unsigned 64-bit number.  We flip the
// highest bit, so that negative input values map to unsigned numbers which are
// less than non-negative input values.
func s2u64(val int64) uint64 {
	ret := uint64(val)
	ret ^= 0x8000000000000000
	return ret
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
	batch.Put(makeKey(SPAN_ID_INDEX_PREFIX, span.Id.Val()), spanDataBuf.Bytes())

	// Add this to the parent index.
	for parentIdx := range span.Parents {
		batch.Put(makeSecondaryKey(PARENT_ID_INDEX_PREFIX,
			span.Parents[parentIdx].Val(), span.Id.Val()), EMPTY_BYTE_BUF)
	}

	// Add to the other secondary indices.
	batch.Put(makeSecondaryKey(BEGIN_TIME_INDEX_PREFIX, s2u64(span.Begin),
		span.Id.Val()), EMPTY_BYTE_BUF)
	batch.Put(makeSecondaryKey(END_TIME_INDEX_PREFIX, s2u64(span.End),
		span.Id.Val()), EMPTY_BYTE_BUF)
	batch.Put(makeSecondaryKey(DURATION_INDEX_PREFIX, s2u64(span.Duration()),
		span.Id.Val()), EMPTY_BYTE_BUF)

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

func (shd *shard) FindChildren(sid common.SpanId, childIds []common.SpanId,
	lim int32) ([]common.SpanId, int32, error) {
	searchKey := makeKey('p', sid.Val())
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
		id := common.SpanId(keyToInt(key[9:]))
		childIds = append(childIds, id)
		lim--
		iter.Next()
	}
	return childIds, lim, nil
}

// Close a shard.
func (shd *shard) Close() {
	lg := shd.store.lg
	shd.incoming <- nil
	lg.Infof("Waiting for %s to exit...\n", shd.path)
	if shd.exited != nil {
		<-shd.exited
	}
	shd.ldb.Close()
	lg.Infof("Closed %s...\n", shd.path)
}

// The Data Store.
type dataStore struct {
	lg *common.Logger

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
	lg := common.NewLogger("datastore", cnf)
	store := &dataStore{lg: lg, shards: []*shard{}, WrittenSpans: writtenSpans}
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
				lg.Error("Error: path " + path + "already exists.")
				return nil, err
			} else {
				err = os.RemoveAll(path)
				if err != nil {
					lg.Error("Failed to create " + path + ": " + err.Error())
					return nil, err
				}
				lg.Info("Cleared " + path)
			}
		}
		var shd *shard
		shd, err = CreateShard(store, cnf, path)
		if err != nil {
			lg.Errorf("Error creating shard %s: %s", path, err.Error())
			return nil, err
		}
		store.shards = append(store.shards, shd)
	}
	meta := &dataStoreMetadata{Version: DATA_STORE_VERSION}
	for idx := range store.shards {
		shd := store.shards[idx]
		err := shd.WriteMetadata(meta)
		if err != nil {
			lg.Error("Failed to write metadata to " + store.shards[idx].path + ": " + err.Error())
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
		store.lg.Errorf("LevelDB failed to open %s: %s\n", path, err.Error())
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
	store.lg.Infof("LevelDB opened %s\n", path)
	return shd, nil
}

func (store *dataStore) GetStatistics() *Statistics {
	return store.stats.Copy()
}

// Close the DataStore.
func (store *dataStore) Close() {
	for idx := range store.shards {
		store.shards[idx].Close()
		store.shards[idx] = nil
	}
	if store.readOpts != nil {
		store.readOpts.Close()
		store.readOpts = nil
	}
	if store.writeOpts != nil {
		store.writeOpts.Close()
		store.writeOpts = nil
	}
	if store.lg != nil {
		store.lg.Close()
		store.lg = nil
	}
}

// Get the index of the shard which stores the given spanId.
func (store *dataStore) getShardIndex(sid common.SpanId) int {
	return int(sid.Val() % uint64(len(store.shards)))
}

func (store *dataStore) WriteSpan(span *common.Span) {
	store.shards[store.getShardIndex(span.Id)].incoming <- span
}

func (store *dataStore) FindSpan(sid common.SpanId) *common.Span {
	return store.shards[store.getShardIndex(sid)].FindSpan(sid)
}

func (shd *shard) FindSpan(sid common.SpanId) *common.Span {
	lg := shd.store.lg
	buf, err := shd.ldb.Get(shd.store.readOpts, makeKey('s', sid.Val()))
	if err != nil {
		if strings.Index(err.Error(), "NotFound:") != -1 {
			return nil
		}
		lg.Warnf("Shard(%s): FindSpan(%016x) error: %s\n",
			shd.path, sid, err.Error())
		return nil
	}
	var span *common.Span
	span, err = shd.decodeSpan(sid, buf)
	if err != nil {
		lg.Errorf("Shard(%s): FindSpan(%016x) decode error: %s\n",
			shd.path, sid, err.Error())
		return nil
	}
	return span
}

func (shd *shard) decodeSpan(sid common.SpanId, buf []byte) (*common.Span, error) {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	data := common.SpanData{}
	err := decoder.Decode(&data)
	if err != nil {
		return nil, err
	}
	// Gob encoding translates empty slices to nil.  Reverse this so that we're always dealing with
	// non-nil slices.
	if data.Parents == nil {
		data.Parents = []common.SpanId{}
	}
	return &common.Span{Id: common.SpanId(sid), SpanData: data}, nil
}

// Find the children of a given span id.
func (store *dataStore) FindChildren(sid common.SpanId, lim int32) []common.SpanId {
	childIds := make([]common.SpanId, 0)
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
			store.lg.Errorf("Shard(%s): FindChildren(%016x) error: %s\n",
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

type predicateData struct {
	*common.Predicate
	uintKey uint64
	strKey  string
}

func loadPredicateData(pred *common.Predicate) (*predicateData, error) {
	p := predicateData{Predicate: pred}

	// Parse the input value given to make sure it matches up with the field
	// type.
	switch pred.Field {
	case common.SPAN_ID:
		// Span IDs are sent as hex strings.
		var id common.SpanId
		if err := id.FromString(pred.Val); err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse span id '%s': %s",
				pred.Val, err.Error()))
		}
		p.uintKey = id.Val()
		break
	case common.DESCRIPTION:
		// Any string is valid for a description.
		p.strKey = pred.Val
		break
	case common.BEGIN_TIME, common.END_TIME, common.DURATION:
		// Parse a base-10 signed numeric field.
		v, err := strconv.ParseInt(pred.Val, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse %s '%s': %s",
				pred.Field, pred.Val, err.Error()))
		}
		p.uintKey = s2u64(v)
		break
	default:
		return nil, errors.New(fmt.Sprintf("Unknown field %s", pred.Field))
	}

	// Validate the predicate operation.
	switch pred.Op {
	case common.EQUALS, common.LESS_THAN_OR_EQUALS,
		common.GREATER_THAN_OR_EQUALS, common.GREATER_THAN:
		break
	case common.CONTAINS:
		if p.fieldIsNumeric() {
			return nil, errors.New(fmt.Sprintf("Can't use CONTAINS on a "+
				"numeric field like '%s'", pred.Field))
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unknown predicate operation '%s'",
			pred.Op))
	}

	return &p, nil
}

// Get the index prefix for this predicate, or 0 if it is not indexed.
func (pred *predicateData) getIndexPrefix() byte {
	switch pred.Field {
	case common.SPAN_ID:
		return SPAN_ID_INDEX_PREFIX
	case common.BEGIN_TIME:
		return BEGIN_TIME_INDEX_PREFIX
	case common.END_TIME:
		return END_TIME_INDEX_PREFIX
	case common.DURATION:
		return DURATION_INDEX_PREFIX
	default:
		return INVALID_INDEX_PREFIX
	}
}

// Returns true if the predicate type is numeric.
func (pred *predicateData) fieldIsNumeric() bool {
	switch pred.Field {
	case common.SPAN_ID, common.BEGIN_TIME, common.END_TIME, common.DURATION:
		return true
	default:
		return false
	}
}

// Get the values that this predicate cares about for a given span.
func (pred *predicateData) extractRelevantSpanData(span *common.Span) (uint64, string) {
	switch pred.Field {
	case common.SPAN_ID:
		return span.Id.Val(), ""
	case common.DESCRIPTION:
		return 0, span.Description
	case common.BEGIN_TIME:
		return s2u64(span.Begin), ""
	case common.END_TIME:
		return s2u64(span.End), ""
	case common.DURATION:
		return s2u64(span.Duration()), ""
	default:
		panic(fmt.Sprintf("Field type %s isn't a 64-bit integer.", pred.Field))
	}
}

func (pred *predicateData) spanPtrIsBefore(a *common.Span, b *common.Span) bool {
	// nil is after everything.
	if a == nil {
		if b == nil {
			return false
		}
		return false
	} else if b == nil {
		return true
	}
	// Compare the spans according to this predicate.
	aInt, aStr := pred.extractRelevantSpanData(a)
	bInt, bStr := pred.extractRelevantSpanData(b)
	if pred.fieldIsNumeric() {
		if pred.Op.IsDescending() {
			return aInt > bInt
		} else {
			return aInt < bInt
		}
	} else {
		if pred.Op.IsDescending() {
			return aStr > bStr
		} else {
			return aStr < bStr
		}
	}
}

// Returns true if the predicate is satisfied by the given span.
func (pred *predicateData) satisfiedBy(span *common.Span) bool {
	intVal, strVal := pred.extractRelevantSpanData(span)
	if pred.fieldIsNumeric() {
		switch pred.Op {
		case common.EQUALS:
			return intVal == pred.uintKey
		case common.LESS_THAN_OR_EQUALS:
			return intVal <= pred.uintKey
		case common.GREATER_THAN_OR_EQUALS:
			return intVal >= pred.uintKey
		case common.GREATER_THAN:
			return intVal > pred.uintKey
		default:
			panic(fmt.Sprintf("unknown Op type %s should have been caught "+
				"during normalization", pred.Op))
		}
	} else {
		switch pred.Op {
		case common.CONTAINS:
			return strings.Contains(strVal, pred.strKey)
		case common.EQUALS:
			return strVal == pred.strKey
		case common.LESS_THAN_OR_EQUALS:
			return strVal <= pred.strKey
		case common.GREATER_THAN_OR_EQUALS:
			return strVal >= pred.strKey
		case common.GREATER_THAN:
			return strVal > pred.strKey
		default:
			panic(fmt.Sprintf("unknown Op type %s should have been caught "+
				"during normalization", pred.Op))
		}
	}
}

func (pred *predicateData) createSource(store *dataStore) (*source, error) {
	var ret *source
	src := source{store: store,
		pred:      pred,
		iters:     make([]*levigo.Iterator, 0, len(store.shards)),
		nexts:     make([]*common.Span, len(store.shards)),
		numRead:   make([]int, len(store.shards)),
		keyPrefix: pred.getIndexPrefix(),
	}
	if src.keyPrefix == INVALID_INDEX_PREFIX {
		return nil, errors.New(fmt.Sprintf("Can't create source from unindexed "+
			"predicate on field %s", pred.Field))
	}
	defer func() {
		if ret == nil {
			src.Close()
		}
	}()
	for shardIdx := range store.shards {
		shd := store.shards[shardIdx]
		src.iters = append(src.iters, shd.ldb.NewIterator(store.readOpts))
	}
	searchKey := makeKey(src.keyPrefix, pred.uintKey)
	for i := range src.iters {
		src.iters[i].Seek(searchKey)
	}
	ret = &src
	return ret, nil
}

// A source of spans.
type source struct {
	store     *dataStore
	pred      *predicateData
	iters     []*levigo.Iterator
	nexts     []*common.Span
	numRead   []int
	keyPrefix byte
}

// Return true if this operation may require skipping the first result we get back from leveldb.
func mayRequireOneSkip(op common.Op) bool {
	switch op {
	// When dealing with descending predicates, the first span we read might not satisfy
	// the predicate, even though subsequent ones will.  This is because the iter.Seek()
	// function "moves the iterator the position of the key given or, if the key doesn't
	// exist, the next key that does exist in the database."  So if we're on that "next
	// key" it will not satisfy the predicate, but the keys previous to it might.
	case common.LESS_THAN_OR_EQUALS:
		return true
	// iter.Seek basically takes us to the key which is "greater than or equal to" some
	// value.  Since we want greater than (not greater than or equal to) we may have to
	// skip the first key.
	case common.GREATER_THAN:
		return true
	}
	return false
}

// Fill in the entry in the 'next' array for a specific shard.
func (src *source) populateNextFromShard(shardIdx int) {
	lg := src.store.lg
	var err error
	iter := src.iters[shardIdx]
	if iter == nil {
		lg.Debugf("Can't populate: No more entries in shard %d\n", shardIdx)
		return // There are no more entries in this shard.
	}
	if src.nexts[shardIdx] != nil {
		lg.Debugf("No need to populate shard %d\n", shardIdx)
		return // We already have a valid entry for this shard.
	}
	for {
		if !iter.Valid() {
			lg.Debugf("Can't populate: Iterator for shard %d is no longer valid.\n", shardIdx)
			break // Can't read past end of DB
		}
		src.numRead[shardIdx]++
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte{src.keyPrefix}) {
			lg.Debugf("Can't populate: Iterator for shard %d does not have prefix %s",
				shardIdx, string(src.keyPrefix))
			break // Can't read past end of indexed section
		}
		var span *common.Span
		var sid common.SpanId
		if src.keyPrefix == SPAN_ID_INDEX_PREFIX {
			// The span id maps to the span itself.
			sid = common.SpanId(keyToInt(key[1:]))
			span, err = src.store.shards[shardIdx].decodeSpan(sid, iter.Value())
			if err != nil {
				lg.Debugf("Internal error decoding span %s in shard %d: %s\n",
					sid.String(), shardIdx, err.Error())
				break
			}
		} else {
			// With a secondary index, we have to look up the span by id.
			sid = common.SpanId(keyToInt(key[9:]))
			span = src.store.shards[shardIdx].FindSpan(sid)
			if span == nil {
				lg.Debugf("Internal error rehydrating span %s in shard %d\n",
					sid.String(), shardIdx)
				break
			}
		}
		if src.pred.Op.IsDescending() {
			iter.Prev()
		} else {
			iter.Next()
		}
		if src.pred.satisfiedBy(span) {
			lg.Debugf("Populated valid span %016x from shard %d.\n", sid, shardIdx)
			src.nexts[shardIdx] = span // Found valid entry
			return
		} else {
			lg.Debugf("Span %016x from shard %d does not satisfy the predicate.\n",
				sid, shardIdx)
			if src.numRead[shardIdx] <= 1 && mayRequireOneSkip(src.pred.Op) {
				continue
			}
			// This and subsequent entries don't satisfy predicate
			break
		}
	}
	lg.Debugf("Closing iterator for shard %d.\n", shardIdx)
	iter.Close()
	src.iters[shardIdx] = nil
}

func (src *source) next() *common.Span {
	for shardIdx := range src.iters {
		src.populateNextFromShard(shardIdx)
	}
	var best *common.Span
	bestIdx := -1
	for shardIdx := range src.iters {
		span := src.nexts[shardIdx]
		if src.pred.spanPtrIsBefore(span, best) {
			best = span
			bestIdx = shardIdx
		}
	}
	if bestIdx >= 0 {
		src.nexts[bestIdx] = nil
	}
	return best
}

func (src *source) Close() {
	for i := range src.iters {
		if src.iters[i] != nil {
			src.iters[i].Close()
		}
	}
	src.iters = nil
}

func (store *dataStore) obtainSource(preds *[]*predicateData) (*source, error) {
	// Read spans from the first predicate that is indexed.
	p := *preds
	for i := range p {
		pred := p[i]
		if pred.getIndexPrefix() != INVALID_INDEX_PREFIX {
			*preds = append(p[0:i], p[i+1:]...)
			return pred.createSource(store)
		}
	}
	// If there are no predicates that are indexed, read rows in order of span id.
	spanIdPred := common.Predicate{Op: common.GREATER_THAN_OR_EQUALS,
		Field: common.SPAN_ID,
		Val:   "0000000000000000",
	}
	spanIdPredData, err := loadPredicateData(&spanIdPred)
	if err != nil {
		return nil, err
	}
	return spanIdPredData.createSource(store)
}

func (store *dataStore) HandleQuery(query *common.Query) ([]*common.Span, error) {
	lg := store.lg
	// Parse predicate data.
	var err error
	preds := make([]*predicateData, len(query.Predicates))
	for i := range query.Predicates {
		preds[i], err = loadPredicateData(&query.Predicates[i])
		if err != nil {
			return nil, err
		}
	}
	// Get a source of rows.
	var src *source
	src, err = store.obtainSource(&preds)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	lg.Debugf("HandleQuery %s: preds = %s, src = %v\n", query, preds, src)

	// Filter the spans through the remaining predicates.
	ret := make([]*common.Span, 0, 32)
	for {
		if len(ret) >= query.Lim {
			break // we hit the result size limit
		}
		span := src.next()
		if span == nil {
			break // the source has no more spans to give
		}
		lg.Debugf("src.next returned span %s\n", span.ToJson())
		satisfied := true
		for predIdx := range preds {
			if !preds[predIdx].satisfiedBy(span) {
				satisfied = false
				break
			}
		}
		if satisfied {
			ret = append(ret, span)
		}
	}
	return ret, nil
}
