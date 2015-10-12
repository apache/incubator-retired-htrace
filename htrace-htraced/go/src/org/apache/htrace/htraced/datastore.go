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
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
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
// m -> dataStoreVersion
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

const UNKNOWN_LAYOUT_VERSION = 0
const CURRENT_LAYOUT_VERSION = 2

var EMPTY_BYTE_BUF []byte = []byte{}

const VERSION_KEY = 'v'

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

func u64toSlice(val uint64) []byte {
	return []byte{
		byte(0xff & (val >> 56)),
		byte(0xff & (val >> 48)),
		byte(0xff & (val >> 40)),
		byte(0xff & (val >> 32)),
		byte(0xff & (val >> 24)),
		byte(0xff & (val >> 16)),
		byte(0xff & (val >> 8)),
		byte(0xff & (val >> 0))}
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
	primaryKey :=
		append([]byte{SPAN_ID_INDEX_PREFIX}, span.Id.Val()...)
	batch.Put(primaryKey, spanDataBuf.Bytes())

	// Add this to the parent index.
	for parentIdx := range span.Parents {
		key := append(append([]byte{PARENT_ID_INDEX_PREFIX},
			span.Parents[parentIdx].Val()...), span.Id.Val()...)
		batch.Put(key, EMPTY_BYTE_BUF)
	}

	// Add to the other secondary indices.
	beginTimeKey := append(append([]byte{BEGIN_TIME_INDEX_PREFIX},
		u64toSlice(s2u64(span.Begin))...), span.Id.Val()...)
	batch.Put(beginTimeKey, EMPTY_BYTE_BUF)
	endTimeKey := append(append([]byte{END_TIME_INDEX_PREFIX},
		u64toSlice(s2u64(span.End))...), span.Id.Val()...)
	batch.Put(endTimeKey, EMPTY_BYTE_BUF)
	durationKey := append(append([]byte{DURATION_INDEX_PREFIX},
		u64toSlice(s2u64(span.Duration()))...), span.Id.Val()...)
	batch.Put(durationKey, EMPTY_BYTE_BUF)

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
	searchKey := append([]byte{PARENT_ID_INDEX_PREFIX}, sid.Val()...)
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
		id := common.SpanId(key[17:])
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

	var err error
	lg := common.NewLogger("datastore", cnf)
	store := &dataStore{lg: lg, shards: []*shard{}, WrittenSpans: writtenSpans}

	// If we return an error, close the store.
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
		var shd *shard
		shd, err = CreateShard(store, cnf, path, clearStored)
		if err != nil {
			lg.Errorf("Error creating shard %s: %s\n", path, err.Error())
			return nil, err
		}
		store.shards = append(store.shards, shd)
	}
	for idx := range store.shards {
		shd := store.shards[idx]
		shd.exited = make(chan bool, 1)
		go shd.processIncoming()
	}
	return store, nil
}

func CreateShard(store *dataStore, cnf *conf.Config, path string,
	clearStored bool) (*shard, error) {
	lg := store.lg
	if clearStored {
		fi, err := os.Stat(path)
		if err != nil && !os.IsNotExist(err) {
			lg.Errorf("Failed to stat %s: %s\n", path, err.Error())
			return nil, err
		}
		if fi != nil {
			err = os.RemoveAll(path)
			if err != nil {
				lg.Errorf("Failed to clear existing datastore directory %s: %s\n",
					path, err.Error())
				return nil, err
			}
			lg.Infof("Cleared existing datastore directory %s\n", path)
		}
	}
	err := os.MkdirAll(path, 0777)
	if err != nil {
		lg.Errorf("Failed to MkdirAll(%s): %s\n", path, err.Error())
		return nil, err
	}
	var shd *shard
	openOpts := levigo.NewOptions()
	defer openOpts.Close()
	newlyCreated := false
	ldb, err := levigo.Open(path, openOpts)
	if err == nil {
		store.lg.Infof("LevelDB opened %s\n", path)
	} else {
		store.lg.Debugf("LevelDB failed to open %s: %s\n", path, err.Error())
		openOpts.SetCreateIfMissing(true)
		ldb, err = levigo.Open(path, openOpts)
		if err != nil {
			store.lg.Errorf("LevelDB failed to create %s: %s\n", path, err.Error())
			return nil, err
		}
		store.lg.Infof("Created new LevelDB instance in %s\n", path)
		newlyCreated = true
	}
	defer func() {
		if shd == nil {
			ldb.Close()
		}
	}()
	lv, err := readLayoutVersion(store, ldb)
	if err != nil {
		store.lg.Errorf("Got error while reading datastore version for %s: %s\n",
			path, err.Error())
		return nil, err
	}
	if newlyCreated && (lv == UNKNOWN_LAYOUT_VERSION) {
		err = writeDataStoreVersion(store, ldb, CURRENT_LAYOUT_VERSION)
		if err != nil {
			store.lg.Errorf("Got error while writing datastore version for %s: %s\n",
				path, err.Error())
			return nil, err
		}
		store.lg.Tracef("Wrote layout version %d to shard at %s.\n",
			CURRENT_LAYOUT_VERSION, path)
	} else if lv != CURRENT_LAYOUT_VERSION {
		versionName := "unknown"
		if lv != UNKNOWN_LAYOUT_VERSION {
			versionName = fmt.Sprintf("%d", lv)
		}
		store.lg.Errorf("Can't read old datastore.  Its layout version is %s, but this "+
			"software is at layout version %d.  Please set %s to clear the datastore "+
			"on startup, or clear it manually.\n", versionName,
			CURRENT_LAYOUT_VERSION, conf.HTRACE_DATA_STORE_CLEAR)
		return nil, errors.New(fmt.Sprintf("Invalid layout version: got %s, expected %d.",
			versionName, CURRENT_LAYOUT_VERSION))
	} else {
		store.lg.Tracef("Found layout version %d in %s.\n", lv, path)
	}
	spanBufferSize := cnf.GetInt(conf.HTRACE_DATA_STORE_SPAN_BUFFER_SIZE)
	shd = &shard{store: store, ldb: ldb, path: path,
		incoming: make(chan *common.Span, spanBufferSize)}
	return shd, nil
}

// Read the datastore version of a leveldb instance.
func readLayoutVersion(store *dataStore, ldb *levigo.DB) (uint32, error) {
	buf, err := ldb.Get(store.readOpts, []byte{VERSION_KEY})
	if err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	var v uint32
	err = decoder.Decode(&v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Write the datastore version to a shard.
func writeDataStoreVersion(store *dataStore, ldb *levigo.DB, v uint32) error {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(&v)
	if err != nil {
		return err
	}
	return ldb.Put(store.writeOpts, []byte{VERSION_KEY}, w.Bytes())
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
	return int(sid.Hash32() % uint32(len(store.shards)))
}

func (store *dataStore) WriteSpan(span *common.Span) {
	store.shards[store.getShardIndex(span.Id)].incoming <- span
}

func (store *dataStore) FindSpan(sid common.SpanId) *common.Span {
	return store.shards[store.getShardIndex(sid)].FindSpan(sid)
}

func (shd *shard) FindSpan(sid common.SpanId) *common.Span {
	lg := shd.store.lg
	primaryKey := append([]byte{SPAN_ID_INDEX_PREFIX}, sid.Val()...)
	buf, err := shd.ldb.Get(shd.store.readOpts, primaryKey)
	if err != nil {
		if strings.Index(err.Error(), "NotFound:") != -1 {
			return nil
		}
		lg.Warnf("Shard(%s): FindSpan(%s) error: %s\n",
			shd.path, sid.String(), err.Error())
		return nil
	}
	var span *common.Span
	span, err = shd.decodeSpan(sid, buf)
	if err != nil {
		lg.Errorf("Shard(%s): FindSpan(%s) decode error: %s\n",
			shd.path, sid.String(), err.Error())
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
			store.lg.Errorf("Shard(%s): FindChildren(%s) error: %s\n",
				shd.path, sid.String(), err.Error())
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
	key []byte
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
		p.key = id.Val()
		break
	case common.DESCRIPTION:
		// Any string is valid for a description.
		p.key = []byte(pred.Val)
		break
	case common.BEGIN_TIME, common.END_TIME, common.DURATION:
		// Parse a base-10 signed numeric field.
		v, err := strconv.ParseInt(pred.Val, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse %s '%s': %s",
				pred.Field, pred.Val, err.Error()))
		}
		p.key = u64toSlice(s2u64(v))
		break
	case common.TRACER_ID:
		// Any string is valid for a tracer ID.
		p.key = []byte(pred.Val)
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
func (pred *predicateData) extractRelevantSpanData(span *common.Span) []byte {
	switch pred.Field {
	case common.SPAN_ID:
		return span.Id.Val()
	case common.DESCRIPTION:
		return []byte(span.Description)
	case common.BEGIN_TIME:
		return u64toSlice(s2u64(span.Begin))
	case common.END_TIME:
		return u64toSlice(s2u64(span.End))
	case common.DURATION:
		return u64toSlice(s2u64(span.Duration()))
	case common.TRACER_ID:
		return []byte(span.TracerId)
	default:
		panic(fmt.Sprintf("Unknown field type %s.", pred.Field))
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
	aVal := pred.extractRelevantSpanData(a)
	bVal := pred.extractRelevantSpanData(b)
	cmp := bytes.Compare(aVal, bVal)
	if pred.Op.IsDescending() {
		return cmp > 0
	} else {
		return cmp < 0
	}
}

// Returns true if the predicate is satisfied by the given span.
func (pred *predicateData) satisfiedBy(span *common.Span) bool {
	val := pred.extractRelevantSpanData(span)
	switch pred.Op {
	case common.CONTAINS:
		return bytes.Contains(val, pred.key)
	case common.EQUALS:
		return bytes.Equal(val, pred.key)
	case common.LESS_THAN_OR_EQUALS:
		return bytes.Compare(val, pred.key) <= 0
	case common.GREATER_THAN_OR_EQUALS:
		return bytes.Compare(val, pred.key) >= 0
	case common.GREATER_THAN:
		return bytes.Compare(val, pred.key) > 0
	default:
		panic(fmt.Sprintf("unknown Op type %s should have been caught "+
			"during normalization", pred.Op))
	}
}

func (pred *predicateData) createSource(store *dataStore, prev *common.Span) (*source, error) {
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
	var searchKey []byte
	lg := store.lg
	if prev != nil {
		// If prev != nil, this query RPC is the continuation of a previous
		// one.  The final result returned the last time is 'prev'.
		//
		// To avoid returning the same results multiple times, we adjust the
		// predicate here.  If the predicate is on the span id field, we
		// simply manipulate the span ID we're looking for.
		//
		// If the predicate is on a secondary index, we also use span ID, but
		// in a slightly different way.  Since the secondary indices are
		// organized as [type-code][8b-secondary-key][8b-span-id], elements
		// with the same secondary index field are ordered by span ID.  So we
		// create a 17-byte key incorporating the span ID from 'prev.'
		startId := common.INVALID_SPAN_ID
		switch pred.Op {
		case common.EQUALS:
			if pred.Field == common.SPAN_ID {
				// This is an annoying corner case.  There can only be one
				// result each time we do an EQUALS search for a span id.
				// Span id is the primary key for all our spans.
				// But for some reason someone is asking for another result.
				// We modify the query to search for the illegal 0 span ID,
				// which will never be present.
				lg.Debugf("Attempted to use a continuation token with an EQUALS "+
					"SPAN_ID query. %s.  Setting search id = 0",
					pred.Predicate.String())
				startId = common.INVALID_SPAN_ID
			} else {
				// When doing an EQUALS search on a secondary index, the
				// results are sorted by span id.
				startId = prev.Id.Next()
			}
		case common.LESS_THAN_OR_EQUALS:
			// Subtract one from the previous span id.  Since the previous
			// start ID will never be 0 (0 is an illegal span id), we'll never
			// wrap around when doing this.
			startId = prev.Id.Prev()
		case common.GREATER_THAN_OR_EQUALS:
			// We can't add one to the span id, since the previous span ID
			// might be the maximum value.  So just switch over to using
			// GREATER_THAN.
			pred.Op = common.GREATER_THAN
			startId = prev.Id
		case common.GREATER_THAN:
			// This one is easy.
			startId = prev.Id
		default:
			str := fmt.Sprintf("Can't use a %v predicate as a source.", pred.Predicate.String())
			lg.Error(str + "\n")
			panic(str)
		}
		if pred.Field == common.SPAN_ID {
			pred.key = startId.Val()
			searchKey = append([]byte{src.keyPrefix}, startId.Val()...)
		} else {
			// Start where the previous query left off.  This means adjusting
			// our uintKey.
			pred.key = pred.extractRelevantSpanData(prev)
			searchKey = append(append([]byte{src.keyPrefix}, pred.key...),
				startId.Val()...)
		}
		if lg.TraceEnabled() {
			lg.Tracef("Handling continuation token %s for %s.  startId=%d, "+
				"pred.uintKey=%s\n", prev, pred.Predicate.String(), startId,
				hex.EncodeToString(pred.key))
		}
	} else {
		searchKey = append([]byte{src.keyPrefix}, pred.key...)
	}
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
			lg.Debugf("Can't populate: Iterator for shard %d does not have prefix %s\n",
				shardIdx, string(src.keyPrefix))
			break // Can't read past end of indexed section
		}
		var span *common.Span
		var sid common.SpanId
		if src.keyPrefix == SPAN_ID_INDEX_PREFIX {
			// The span id maps to the span itself.
			sid = common.SpanId(key[1:17])
			span, err = src.store.shards[shardIdx].decodeSpan(sid, iter.Value())
			if err != nil {
				lg.Debugf("Internal error decoding span %s in shard %d: %s\n",
					sid.String(), shardIdx, err.Error())
				break
			}
		} else {
			// With a secondary index, we have to look up the span by id.
			sid = common.SpanId(key[9:25])
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
			lg.Debugf("Populated valid span %v from shard %d.\n", sid, shardIdx)
			src.nexts[shardIdx] = span // Found valid entry
			return
		} else {
			lg.Debugf("Span %s from shard %d does not satisfy the predicate.\n",
				sid.String(), shardIdx)
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

func (store *dataStore) obtainSource(preds *[]*predicateData, span *common.Span) (*source, error) {
	// Read spans from the first predicate that is indexed.
	p := *preds
	for i := range p {
		pred := p[i]
		if pred.getIndexPrefix() != INVALID_INDEX_PREFIX {
			*preds = append(p[0:i], p[i+1:]...)
			return pred.createSource(store, span)
		}
	}
	// If there are no predicates that are indexed, read rows in order of span id.
	spanIdPred := common.Predicate{Op: common.GREATER_THAN_OR_EQUALS,
		Field: common.SPAN_ID,
		Val:   common.INVALID_SPAN_ID.String(),
	}
	spanIdPredData, err := loadPredicateData(&spanIdPred)
	if err != nil {
		return nil, err
	}
	return spanIdPredData.createSource(store, span)
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
	src, err = store.obtainSource(&preds, query.Prev)
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
		if lg.DebugEnabled() {
			lg.Debugf("src.next returned span %s\n", span.ToJson())
		}
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

func (store *dataStore) ServerStats() *common.ServerStats {
	serverStats := common.ServerStats{
		Shards: make([]common.ShardStats, len(store.shards)),
	}
	for shardIdx := range store.shards {
		shard := store.shards[shardIdx]
		serverStats.Shards[shardIdx].Path = shard.path
		r := levigo.Range{
			Start: append([]byte{SPAN_ID_INDEX_PREFIX},
				common.INVALID_SPAN_ID.Val()...),
			Limit: append([]byte{SPAN_ID_INDEX_PREFIX + 1},
				common.INVALID_SPAN_ID.Val()...),
		}
		vals := shard.ldb.GetApproximateSizes([]levigo.Range{r})
		serverStats.Shards[shardIdx].ApproxNumSpans = vals[0]
		serverStats.Shards[shardIdx].LevelDbStats =
			shard.ldb.PropertyValue("leveldb.stats")
		store.lg.Infof("shard.ldb.PropertyValue(leveldb.stats)=%s\n",
			shard.ldb.PropertyValue("leveldb.stats"))
	}
	return &serverStats
}
