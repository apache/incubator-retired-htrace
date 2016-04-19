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
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/ugorji/go/codec"
	"htrace/common"
	"htrace/conf"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
// losing a little bit of trace data if htraced goes down is not critical.  We use msgpack
// for serialization.  We assume that there will be many more writes than reads.
//
// Schema
// w -> ShardInfo
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

var EMPTY_BYTE_BUF []byte = []byte{}

const SPAN_ID_INDEX_PREFIX = 's'
const BEGIN_TIME_INDEX_PREFIX = 'b'
const END_TIME_INDEX_PREFIX = 'e'
const DURATION_INDEX_PREFIX = 'd'
const PARENT_ID_INDEX_PREFIX = 'p'
const INVALID_INDEX_PREFIX = 0

// The maximum span expiry time, in milliseconds.
// For all practical purposes this is "never" since it's more than a million years.
const MAX_SPAN_EXPIRY_MS = 0x7ffffffffffffff

type IncomingSpan struct {
	// The address that the span was sent from.
	Addr string

	// The span.
	*common.Span

	// Serialized span data
	SpanDataBytes []byte
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
	incoming chan []*IncomingSpan

	// A channel for incoming heartbeats
	heartbeats chan interface{}

	// Tracks whether the shard goroutine has exited.
	exited sync.WaitGroup
}

// Process incoming spans for a shard.
func (shd *shard) processIncoming() {
	lg := shd.store.lg
	defer func() {
		lg.Infof("Shard processor for %s exiting.\n", shd.path)
		shd.exited.Done()
	}()
	for {
		select {
		case spans := <-shd.incoming:
			if spans == nil {
				return
			}
			totalWritten := 0
			totalDropped := 0
			for spanIdx := range spans {
				err := shd.writeSpan(spans[spanIdx])
				if err != nil {
					lg.Errorf("Shard processor for %s got fatal error %s.\n",
						shd.path, err.Error())
					totalDropped++
				} else {
					if lg.TraceEnabled() {
						lg.Tracef("Shard processor for %s wrote span %s.\n",
							shd.path, spans[spanIdx].ToJson())
					}
					totalWritten++
				}
			}
			shd.store.msink.UpdatePersisted(spans[0].Addr, totalWritten, totalDropped)
			if shd.store.WrittenSpans != nil {
				lg.Debugf("Shard %s incrementing WrittenSpans by %d\n", shd.path, len(spans))
				shd.store.WrittenSpans.Posts(int64(len(spans)))
			}
		case <-shd.heartbeats:
			lg.Tracef("Shard processor for %s handling heartbeat.\n", shd.path)
			shd.pruneExpired()
		}
	}
}

func (shd *shard) pruneExpired() {
	lg := shd.store.rpr.lg
	src, err := CreateReaperSource(shd)
	if err != nil {
		lg.Errorf("Error creating reaper source for shd(%s): %s\n",
			shd.path, err.Error())
		return
	}
	var totalReaped uint64
	defer func() {
		src.Close()
		if totalReaped > 0 {
			atomic.AddUint64(&shd.store.rpr.ReapedSpans, totalReaped)
		}
	}()
	urdate := s2u64(shd.store.rpr.GetReaperDate())
	for {
		span := src.next()
		if span == nil {
			lg.Debugf("After reaping %d span(s), no more found in shard %s "+
				"to reap.\n", totalReaped, shd.path)
			return
		}
		begin := s2u64(span.Begin)
		if begin >= urdate {
			lg.Debugf("After reaping %d span(s), the remaining spans in "+
				"shard %s are new enough to be kept\n",
				totalReaped, shd.path)
			return
		}
		err = shd.DeleteSpan(span)
		if err != nil {
			lg.Errorf("Error deleting span %s from shd(%s): %s\n",
				span.String(), shd.path, err.Error())
			return
		}
		if lg.TraceEnabled() {
			lg.Tracef("Reaped span %s from shard %s\n", span.String(), shd.path)
		}
		totalReaped++
	}
}

// Delete a span from the shard.  Note that leveldb may retain the data until
// compaction(s) remove it.
func (shd *shard) DeleteSpan(span *common.Span) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	primaryKey :=
		append([]byte{SPAN_ID_INDEX_PREFIX}, span.Id.Val()...)
	batch.Delete(primaryKey)
	for parentIdx := range span.Parents {
		key := append(append([]byte{PARENT_ID_INDEX_PREFIX},
			span.Parents[parentIdx].Val()...), span.Id.Val()...)
		batch.Delete(key)
	}
	beginTimeKey := append(append([]byte{BEGIN_TIME_INDEX_PREFIX},
		u64toSlice(s2u64(span.Begin))...), span.Id.Val()...)
	batch.Delete(beginTimeKey)
	endTimeKey := append(append([]byte{END_TIME_INDEX_PREFIX},
		u64toSlice(s2u64(span.End))...), span.Id.Val()...)
	batch.Delete(endTimeKey)
	durationKey := append(append([]byte{DURATION_INDEX_PREFIX},
		u64toSlice(s2u64(span.Duration()))...), span.Id.Val()...)
	batch.Delete(durationKey)
	err := shd.ldb.Write(shd.store.writeOpts, batch)
	if err != nil {
		return err
	}
	return nil
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

func (shd *shard) writeSpan(ispan *IncomingSpan) error {
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	span := ispan.Span
	primaryKey :=
		append([]byte{SPAN_ID_INDEX_PREFIX}, span.Id.Val()...)
	batch.Put(primaryKey, ispan.SpanDataBytes)

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

	err := shd.ldb.Write(shd.store.writeOpts, batch)
	if err != nil {
		shd.store.lg.Errorf("Error writing span %s to leveldb at %s: %s\n",
			span.String(), shd.path, err.Error())
		return err
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
	shd.exited.Wait()
	shd.ldb.Close()
	lg.Infof("Closed %s...\n", shd.path)
}

type Reaper struct {
	// The logger used by the reaper
	lg *common.Logger

	// The number of milliseconds to keep spans around, in milliseconds.
	spanExpiryMs int64

	// The oldest date for which we'll keep spans.
	reaperDate int64

	// A channel used to send heartbeats to the reaper
	heartbeats chan interface{}

	// Tracks whether the reaper goroutine has exited
	exited sync.WaitGroup

	// The lock protecting reaper data.
	lock sync.Mutex

	// The reaper heartbeater
	hb *Heartbeater

	// The total number of spans which have been reaped.
	ReapedSpans uint64
}

func NewReaper(cnf *conf.Config) *Reaper {
	rpr := &Reaper{
		lg:           common.NewLogger("reaper", cnf),
		spanExpiryMs: cnf.GetInt64(conf.HTRACE_SPAN_EXPIRY_MS),
		heartbeats:   make(chan interface{}, 1),
	}
	if rpr.spanExpiryMs >= MAX_SPAN_EXPIRY_MS {
		rpr.spanExpiryMs = MAX_SPAN_EXPIRY_MS
	} else if rpr.spanExpiryMs <= 0 {
		rpr.spanExpiryMs = MAX_SPAN_EXPIRY_MS
	}
	rpr.hb = NewHeartbeater("ReaperHeartbeater",
		cnf.GetInt64(conf.HTRACE_REAPER_HEARTBEAT_PERIOD_MS), rpr.lg)
	rpr.exited.Add(1)
	go rpr.run()
	rpr.hb.AddHeartbeatTarget(&HeartbeatTarget{
		name:       "reaper",
		targetChan: rpr.heartbeats,
	})
	var when string
	if rpr.spanExpiryMs >= MAX_SPAN_EXPIRY_MS {
		when = "never"
	} else {
		when = "after " + time.Duration(rpr.spanExpiryMs).String()
	}
	rpr.lg.Infof("Initializing span reaper: span time out = %s.\n", when)
	return rpr
}

func (rpr *Reaper) run() {
	defer func() {
		rpr.lg.Info("Exiting Reaper goroutine.\n")
		rpr.exited.Done()
	}()

	for {
		_, isOpen := <-rpr.heartbeats
		if !isOpen {
			return
		}
		rpr.handleHeartbeat()
	}
}

func (rpr *Reaper) handleHeartbeat() {
	// TODO: check dataStore fullness
	now := common.TimeToUnixMs(time.Now().UTC())
	d, updated := func() (int64, bool) {
		rpr.lock.Lock()
		defer rpr.lock.Unlock()
		newReaperDate := now - rpr.spanExpiryMs
		if newReaperDate > rpr.reaperDate {
			rpr.reaperDate = newReaperDate
			return rpr.reaperDate, true
		} else {
			return rpr.reaperDate, false
		}
	}()
	if rpr.lg.DebugEnabled() {
		if updated {
			rpr.lg.Debugf("Updating UTC reaper date to %s.\n",
				common.UnixMsToTime(d).Format(time.RFC3339))
		} else {
			rpr.lg.Debugf("Not updating previous reaperDate of %s.\n",
				common.UnixMsToTime(d).Format(time.RFC3339))
		}
	}
}

func (rpr *Reaper) GetReaperDate() int64 {
	rpr.lock.Lock()
	defer rpr.lock.Unlock()
	return rpr.reaperDate
}

func (rpr *Reaper) SetReaperDate(rdate int64) {
	rpr.lock.Lock()
	defer rpr.lock.Unlock()
	rpr.reaperDate = rdate
}

func (rpr *Reaper) Shutdown() {
	rpr.hb.Shutdown()
	close(rpr.heartbeats)
}

// The Data Store.
type dataStore struct {
	lg *common.Logger

	// The shards which manage our LevelDB instances.
	shards []*shard

	// The read options to use for LevelDB.
	readOpts *levigo.ReadOptions

	// The write options to use for LevelDB.
	writeOpts *levigo.WriteOptions

	// If non-null, a semaphore we will increment once for each span we receive.
	// Used for testing.
	WrittenSpans *common.Semaphore

	// The metrics sink.
	msink *MetricsSink

	// The heartbeater which periodically asks shards to update the MetricsSink.
	hb *Heartbeater

	// The reaper for this datastore
	rpr *Reaper

	// When this datastore was started (in UTC milliseconds since the epoch)
	startMs int64
}

func CreateDataStore(cnf *conf.Config, writtenSpans *common.Semaphore) (*dataStore, error) {
	dld := NewDataStoreLoader(cnf)
	defer dld.Close()
	err := dld.Load()
	if err != nil {
		dld.lg.Errorf("Error loading datastore: %s\n", err.Error())
		return nil, err
	}
	store := &dataStore{
		lg:           dld.lg,
		shards:       make([]*shard, len(dld.shards)),
		readOpts:     dld.readOpts,
		writeOpts:    dld.writeOpts,
		WrittenSpans: writtenSpans,
		msink:        NewMetricsSink(cnf),
		hb: NewHeartbeater("DatastoreHeartbeater",
			cnf.GetInt64(conf.HTRACE_DATASTORE_HEARTBEAT_PERIOD_MS), dld.lg),
		rpr:     NewReaper(cnf),
		startMs: common.TimeToUnixMs(time.Now().UTC()),
	}
	spanBufferSize := cnf.GetInt(conf.HTRACE_DATA_STORE_SPAN_BUFFER_SIZE)
	for shdIdx := range store.shards {
		shd := &shard{
			store:      store,
			ldb:        dld.shards[shdIdx].ldb,
			path:       dld.shards[shdIdx].path,
			incoming:   make(chan []*IncomingSpan, spanBufferSize),
			heartbeats: make(chan interface{}, 1),
		}
		shd.exited.Add(1)
		go shd.processIncoming()
		store.shards[shdIdx] = shd
		store.hb.AddHeartbeatTarget(&HeartbeatTarget{
			name:       fmt.Sprintf("shard(%s)", shd.path),
			targetChan: shd.heartbeats,
		})
	}
	dld.DisownResources()
	return store, nil
}

// Close the DataStore.
func (store *dataStore) Close() {
	if store.hb != nil {
		store.hb.Shutdown()
		store.hb = nil
	}
	for idx := range store.shards {
		if store.shards[idx] != nil {
			store.shards[idx].Close()
			store.shards[idx] = nil
		}
	}
	if store.rpr != nil {
		store.rpr.Shutdown()
		store.rpr = nil
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

const WRITESPANS_BATCH_SIZE = 128

// SpanIngestor is a class used internally to ingest spans from an RPC
// endpoint.  It groups spans destined for a particular shard into small
// batches, so that we can reduce the number of objects that need to be sent
// over the shard's "incoming" channel.  Since sending objects over a channel
// requires goroutine synchronization, this improves performance.
//
// SpanIngestor also allows us to reuse the same encoder object for many spans,
// rather than creating a new encoder per span.  This avoids re-doing the
// encoder setup for each span, and also generates less garbage.
type SpanIngestor struct {
	// The logger to use.
	lg *common.Logger

	// The dataStore we are ingesting spans into.
	store *dataStore

	// The remote address these spans are coming from.
	addr string

	// Default TracerId
	defaultTrid string

	// The msgpack handle to use to serialize the spans.
	mh codec.MsgpackHandle

	// The msgpack encoder to use to serialize the spans.
	// Caching this avoids generating a lot of garbage and burning CPUs
	// creating new encoder objects for each span.
	enc *codec.Encoder

	// The buffer which codec.Encoder is currently serializing to.
	// We have to create a new buffer for each span because once we hand it off to the shard, the
	// shard manages the buffer lifecycle.
	spanDataBytes []byte

	// An array mapping shard index to span batch.
	batches []*SpanIngestorBatch

	// The total number of spans ingested.  Includes dropped spans.
	totalIngested int

	// The total number of spans the ingestor dropped because of a server-side error.
	serverDropped int
}

// A batch of spans destined for a particular shard.
type SpanIngestorBatch struct {
	incoming []*IncomingSpan
}

func (store *dataStore) NewSpanIngestor(lg *common.Logger,
	addr string, defaultTrid string) *SpanIngestor {
	ing := &SpanIngestor{
		lg:            lg,
		store:         store,
		addr:          addr,
		defaultTrid:   defaultTrid,
		spanDataBytes: make([]byte, 0, 1024),
		batches:       make([]*SpanIngestorBatch, len(store.shards)),
	}
	ing.mh.WriteExt = true
	ing.enc = codec.NewEncoderBytes(&ing.spanDataBytes, &ing.mh)
	for batchIdx := range ing.batches {
		ing.batches[batchIdx] = &SpanIngestorBatch{
			incoming: make([]*IncomingSpan, 0, WRITESPANS_BATCH_SIZE),
		}
	}
	return ing
}

func (ing *SpanIngestor) IngestSpan(span *common.Span) {
	ing.totalIngested++
	// Make sure the span ID is valid.
	spanIdProblem := span.Id.FindProblem()
	if spanIdProblem != "" {
		// Can't print the invalid span ID because String() might fail.
		ing.lg.Warnf("Invalid span ID: %s\n", spanIdProblem)
		ing.serverDropped++
		return
	}

	// Set the default tracer id, if needed.
	if span.TracerId == "" {
		span.TracerId = ing.defaultTrid
	}

	// Encode the span data.  Doing the encoding here is better than doing it
	// in the shard goroutine, because we can achieve more parallelism.
	// There is one shard goroutine per shard, but potentially many more
	// ingestors per shard.
	err := ing.enc.Encode(span.SpanData)
	if err != nil {
		ing.lg.Warnf("Failed to encode span ID %s: %s\n",
			span.Id.String(), err.Error())
		ing.serverDropped++
		return
	}
	spanDataBytes := ing.spanDataBytes
	ing.spanDataBytes = make([]byte, 0, 1024)
	ing.enc.ResetBytes(&ing.spanDataBytes)

	// Determine which shard this span should go to.
	shardIdx := ing.store.getShardIndex(span.Id)
	batch := ing.batches[shardIdx]
	incomingLen := len(batch.incoming)
	if ing.lg.TraceEnabled() {
		ing.lg.Tracef("SpanIngestor#IngestSpan: spanId=%s, shardIdx=%d, "+
			"incomingLen=%d, cap(batch.incoming)=%d\n",
			span.Id.String(), shardIdx, incomingLen, cap(batch.incoming))
	}
	if incomingLen+1 == cap(batch.incoming) {
		if ing.lg.TraceEnabled() {
			ing.lg.Tracef("SpanIngestor#IngestSpan: flushing %d spans for "+
				"shard %d\n", len(batch.incoming), shardIdx)
		}
		ing.store.WriteSpans(shardIdx, batch.incoming)
		batch.incoming = make([]*IncomingSpan, 1, WRITESPANS_BATCH_SIZE)
		incomingLen = 0
	} else {
		batch.incoming = batch.incoming[0 : incomingLen+1]
	}
	batch.incoming[incomingLen] = &IncomingSpan{
		Addr:          ing.addr,
		Span:          span,
		SpanDataBytes: spanDataBytes,
	}
}

func (ing *SpanIngestor) Close(startTime time.Time) {
	for shardIdx := range ing.batches {
		batch := ing.batches[shardIdx]
		if len(batch.incoming) > 0 {
			if ing.lg.TraceEnabled() {
				ing.lg.Tracef("SpanIngestor#Close: flushing %d span(s) for "+
					"shard %d\n", len(batch.incoming), shardIdx)
			}
			ing.store.WriteSpans(shardIdx, batch.incoming)
		}
		batch.incoming = nil
	}
	ing.lg.Debugf("Closed span ingestor for %s.  Ingested %d span(s); dropped "+
		"%d span(s).\n", ing.addr, ing.totalIngested, ing.serverDropped)

	endTime := time.Now()
	ing.store.msink.UpdateIngested(ing.addr, ing.totalIngested,
		ing.serverDropped, endTime.Sub(startTime))
}

func (store *dataStore) WriteSpans(shardIdx int, ispans []*IncomingSpan) {
	store.shards[shardIdx].incoming <- ispans
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
		lg.Errorf("Shard(%s): FindSpan(%s) decode error: %s decoding [%s]\n",
			shd.path, sid.String(), err.Error(), hex.EncodeToString(buf))
		return nil
	}
	return span
}

func (shd *shard) decodeSpan(sid common.SpanId, buf []byte) (*common.Span, error) {
	r := bytes.NewBuffer(buf)
	mh := new(codec.MsgpackHandle)
	mh.WriteExt = true
	decoder := codec.NewDecoder(r, mh)
	data := common.SpanData{}
	err := decoder.Decode(&data)
	if err != nil {
		return nil, err
	}
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

type satisfiedByReturn int

const (
	NOT_SATISFIED     satisfiedByReturn = iota
	NOT_YET_SATISFIED                   = iota
	SATISFIED                           = iota
)

func (r satisfiedByReturn) String() string {
	switch r {
	case NOT_SATISFIED:
		return "NOT_SATISFIED"
	case NOT_YET_SATISFIED:
		return "NOT_YET_SATISFIED"
	case SATISFIED:
		return "SATISFIED"
	default:
		return "(unknown)"
	}
}

// Determine whether the predicate is satisfied by the given span.
func (pred *predicateData) satisfiedBy(span *common.Span) satisfiedByReturn {
	val := pred.extractRelevantSpanData(span)
	switch pred.Op {
	case common.CONTAINS:
		if bytes.Contains(val, pred.key) {
			return SATISFIED
		} else {
			return NOT_SATISFIED
		}
	case common.EQUALS:
		if bytes.Equal(val, pred.key) {
			return SATISFIED
		} else {
			return NOT_SATISFIED
		}
	case common.LESS_THAN_OR_EQUALS:
		if bytes.Compare(val, pred.key) <= 0 {
			return SATISFIED
		} else {
			return NOT_YET_SATISFIED
		}
	case common.GREATER_THAN_OR_EQUALS:
		if bytes.Compare(val, pred.key) >= 0 {
			return SATISFIED
		} else {
			return NOT_SATISFIED
		}
	case common.GREATER_THAN:
		cmp := bytes.Compare(val, pred.key)
		if cmp <= 0 {
			return NOT_YET_SATISFIED
		} else {
			return SATISFIED
		}
	default:
		panic(fmt.Sprintf("unknown Op type %s should have been caught "+
			"during normalization", pred.Op))
	}
}

func (pred *predicateData) createSource(store *dataStore, prev *common.Span) (*source, error) {
	var ret *source
	src := source{store: store,
		pred:      pred,
		shards:    make([]*shard, len(store.shards)),
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
		src.shards[shardIdx] = shd
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
				if lg.DebugEnabled() {
					lg.Debugf("Attempted to use a continuation token with an EQUALS "+
						"SPAN_ID query. %s.  Setting search id = 0",
						pred.Predicate.String())
				}
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
	shards    []*shard
	iters     []*levigo.Iterator
	nexts     []*common.Span
	numRead   []int
	keyPrefix byte
}

func CreateReaperSource(shd *shard) (*source, error) {
	store := shd.store
	p := &common.Predicate{
		Op:    common.GREATER_THAN_OR_EQUALS,
		Field: common.BEGIN_TIME,
		Val:   common.INVALID_SPAN_ID.String(),
	}
	pred, err := loadPredicateData(p)
	if err != nil {
		return nil, err
	}
	src := &source{
		store:     store,
		pred:      pred,
		shards:    []*shard{shd},
		iters:     make([]*levigo.Iterator, 1),
		nexts:     make([]*common.Span, 1),
		numRead:   make([]int, 1),
		keyPrefix: pred.getIndexPrefix(),
	}
	iter := shd.ldb.NewIterator(store.readOpts)
	src.iters[0] = iter
	searchKey := append(append([]byte{src.keyPrefix}, pred.key...),
		pred.key...)
	iter.Seek(searchKey)
	return src, nil
}

// Fill in the entry in the 'next' array for a specific shard.
func (src *source) populateNextFromShard(shardIdx int) {
	lg := src.store.lg
	var err error
	iter := src.iters[shardIdx]
	shdPath := src.shards[shardIdx].path
	if iter == nil {
		lg.Debugf("Can't populate: No more entries in shard %s\n", shdPath)
		return // There are no more entries in this shard.
	}
	if src.nexts[shardIdx] != nil {
		lg.Debugf("No need to populate shard %s\n", shdPath)
		return // We already have a valid entry for this shard.
	}
	for {
		if !iter.Valid() {
			lg.Debugf("Can't populate: Iterator for shard %s is no longer valid.\n", shdPath)
			break // Can't read past end of DB
		}
		src.numRead[shardIdx]++
		key := iter.Key()
		if len(key) < 1 {
			lg.Warnf("Encountered invalid zero-byte key in shard %s.\n", shdPath)
			break
		}
		ret := src.checkKeyPrefix(key[0], iter)
		if ret == NOT_SATISFIED {
			break // Can't read past end of indexed section
		} else if ret == NOT_YET_SATISFIED {
			if src.pred.Op.IsDescending() {
				iter.Prev()
			} else {
				iter.Next()
			}
			continue // Try again because we are not yet at the indexed section.
		}
		var span *common.Span
		var sid common.SpanId
		if src.keyPrefix == SPAN_ID_INDEX_PREFIX {
			// The span id maps to the span itself.
			sid = common.SpanId(key[1:17])
			span, err = src.shards[shardIdx].decodeSpan(sid, iter.Value())
			if err != nil {
				if lg.DebugEnabled() {
					lg.Debugf("Internal error decoding span %s in shard %s: %s\n",
						sid.String(), shdPath, err.Error())
				}
				break
			}
		} else {
			// With a secondary index, we have to look up the span by id.
			sid = common.SpanId(key[9:25])
			span = src.shards[shardIdx].FindSpan(sid)
			if span == nil {
				if lg.DebugEnabled() {
					lg.Debugf("Internal error rehydrating span %s in shard %s\n",
						sid.String(), shdPath)
				}
				break
			}
		}
		if src.pred.Op.IsDescending() {
			iter.Prev()
		} else {
			iter.Next()
		}
		ret = src.pred.satisfiedBy(span)
		if ret == SATISFIED {
			if lg.DebugEnabled() {
				lg.Debugf("Populated valid span %v from shard %s.\n", sid, shdPath)
			}
			src.nexts[shardIdx] = span // Found valid entry
			return
		}
		if ret == NOT_SATISFIED {
			// This and subsequent entries don't satisfy predicate
			break
		}
	}
	lg.Debugf("Closing iterator for shard %s.\n", shdPath)
	iter.Close()
	src.iters[shardIdx] = nil
}

// Check the key prefix against the key prefix of the query.
func (src *source) checkKeyPrefix(kp byte, iter *levigo.Iterator) satisfiedByReturn {
	if kp == src.keyPrefix {
		return SATISFIED
	} else if kp < src.keyPrefix {
		if src.pred.Op.IsDescending() {
			return NOT_SATISFIED
		} else {
			return NOT_YET_SATISFIED
		}
	} else {
		if src.pred.Op.IsDescending() {
			return NOT_YET_SATISFIED
		} else {
			return NOT_SATISFIED
		}
	}
}

func (src *source) next() *common.Span {
	for shardIdx := range src.shards {
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

func (src *source) getStats() string {
	ret := fmt.Sprintf("Source stats: pred = %s", src.pred.String())
	prefix := ". "
	for shardIdx := range src.shards {
		next := fmt.Sprintf("%sRead %d spans from %s", prefix,
			src.numRead[shardIdx], src.shards[shardIdx].path)
		prefix = ", "
		ret = ret + next
	}
	return ret
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

func (store *dataStore) HandleQuery(query *common.Query) ([]*common.Span, error, []int) {
	lg := store.lg
	// Parse predicate data.
	var err error
	preds := make([]*predicateData, len(query.Predicates))
	for i := range query.Predicates {
		preds[i], err = loadPredicateData(&query.Predicates[i])
		if err != nil {
			return nil, err, nil
		}
	}
	// Get a source of rows.
	var src *source
	src, err = store.obtainSource(&preds, query.Prev)
	if err != nil {
		return nil, err, nil
	}
	defer src.Close()
	if lg.DebugEnabled() {
		lg.Debugf("HandleQuery %s: preds = %s, src = %v\n", query, preds, src)
	}

	// Filter the spans through the remaining predicates.
	reserved := 32
	if query.Lim < reserved {
		reserved = query.Lim
	}
	ret := make([]*common.Span, 0, reserved)
	for {
		if len(ret) >= query.Lim {
			if lg.DebugEnabled() {
				lg.Debugf("HandleQuery %s: hit query limit after obtaining "+
					"%d results. %s\n.", query, query.Lim, src.getStats())
			}
			break // we hit the result size limit
		}
		span := src.next()
		if span == nil {
			if lg.DebugEnabled() {
				lg.Debugf("HandleQuery %s: found %d result(s), which are "+
					"all that exist. %s\n", query, len(ret), src.getStats())
			}
			break // the source has no more spans to give
		}
		if lg.DebugEnabled() {
			lg.Debugf("src.next returned span %s\n", span.ToJson())
		}
		satisfied := true
		for predIdx := range preds {
			if preds[predIdx].satisfiedBy(span) != SATISFIED {
				satisfied = false
				break
			}
		}
		if satisfied {
			ret = append(ret, span)
		}
	}
	return ret, nil, src.numRead
}

func (store *dataStore) ServerStats() *common.ServerStats {
	serverStats := common.ServerStats{
		Dirs: make([]common.StorageDirectoryStats, len(store.shards)),
	}
	for shardIdx := range store.shards {
		shard := store.shards[shardIdx]
		serverStats.Dirs[shardIdx].Path = shard.path
		r := levigo.Range{
			Start: []byte{0},
			Limit: []byte{0xff},
		}
		vals := shard.ldb.GetApproximateSizes([]levigo.Range{r})
		serverStats.Dirs[shardIdx].ApproximateBytes = vals[0]
		serverStats.Dirs[shardIdx].LevelDbStats =
			shard.ldb.PropertyValue("leveldb.stats")
		store.msink.lg.Debugf("levedb.stats for %s: %s\n",
			shard.path, shard.ldb.PropertyValue("leveldb.stats"))
	}
	serverStats.LastStartMs = store.startMs
	serverStats.CurMs = common.TimeToUnixMs(time.Now().UTC())
	serverStats.ReapedSpans = atomic.LoadUint64(&store.rpr.ReapedSpans)
	store.msink.PopulateServerStats(&serverStats)
	return &serverStats
}
