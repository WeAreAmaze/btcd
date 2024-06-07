// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/internal/treap"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// bulkFetchData is allows a block location to be specified along with the
// index it was requested from.  This in turn allows the bulk data loading
// functions to sort the data accesses based on the location to improve
// performance while keeping track of which result the data is for.
type bulkFetchData struct {
	*blockLocation
	replyIndex int
}

// bulkFetchDataSorter implements sort.Interface to allow a slice of
// bulkFetchData to be sorted.  In particular it sorts by file and then
// offset so that reads from files are grouped and linear.
type bulkFetchDataSorter []bulkFetchData

// Len returns the number of items in the slice.  It is part of the
// sort.Interface implementation.
func (s bulkFetchDataSorter) Len() int {
	return len(s)
}

// Swap swaps the items at the passed indices.  It is part of the
// sort.Interface implementation.
func (s bulkFetchDataSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns whether the item with index i should sort before the item with
// index j.  It is part of the sort.Interface implementation.
func (s bulkFetchDataSorter) Less(i, j int) bool {
	if s[i].blockFileNum < s[j].blockFileNum {
		return true
	}
	if s[i].blockFileNum > s[j].blockFileNum {
		return false
	}

	return s[i].fileOffset < s[j].fileOffset
}

// pendingBlock houses a block that will be written to disk when the database
// transaction is committed.
type pendingBlock struct {
	hash   *chainhash.Hash
	bytes  []byte
	height int32
}

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the database.Tx interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	managed        bool             // Is the transaction managed?
	closed         bool             // Is the transaction closed?
	writable       bool             // Is the transaction writable?
	db             *db              // DB instance the tx was created from.
	snapshot       *dbCacheSnapshot // Underlying snapshot for txns.
	metaBucket     *bucket          // The root metadata bucket.
	blockIdxBucket *bucket          // The block index bucket.

	// Blocks that need to be stored on commit.  The pendingBlocks map is
	// kept to allow quick lookups of pending data by block hash.
	pendingBlocks    map[chainhash.Hash]int
	pendingBlockData []pendingBlock

	// Files that need to be deleted on commit.  These are the files that
	// are marked as files to be deleted during pruning.
	pendingDelFileNums []uint32

	// Keys that need to be stored or deleted on commit.
	pendingKeys   *treap.Mutable
	pendingRemove *treap.Mutable

	// Active iterators that need to be notified when the pending keys have
	// been updated so the cursors can properly handle updates to the
	// transaction state.
	activeIterLock sync.RWMutex
	activeIters    []*treap.Iterator

	// ------------------------------
	mdbRoTx kv.Tx   // Read Only Tx handler of mdbx
	mdbRwTx kv.RwTx // Read Write Tx handler of mdbx
}

// Enforce transaction implements the database.Tx interface.
var _ database.Tx = (*transaction)(nil)

// initialize MDBX txs
func (tx *transaction) initMDBX_txs() (err error) {
	var mdbRwtx kv.RwTx
	var mdbRotx kv.Tx

	if tx.writable {
		mdbRwtx, err = tx.db.cache.mdb.BeginRw(context.Background())
	} else {
		mdbRotx, err = tx.db.cache.mdb.BeginRo(context.Background())
	}
	if err != nil {
		return
	}

	tx.mdbRwTx = mdbRwtx
	tx.mdbRoTx = mdbRotx

	return
}

// removeActiveIter removes the passed iterator from the list of active
// iterators against the pending keys treap.
func (tx *transaction) removeActiveIter(iter *treap.Iterator) {
	// An indexing for loop is intentionally used over a range here as range
	// does not reevaluate the slice on each iteration nor does it adjust
	// the index for the modified slice.
	tx.activeIterLock.Lock()
	for i := 0; i < len(tx.activeIters); i++ {
		if tx.activeIters[i] == iter {
			copy(tx.activeIters[i:], tx.activeIters[i+1:])
			tx.activeIters[len(tx.activeIters)-1] = nil
			tx.activeIters = tx.activeIters[:len(tx.activeIters)-1]
		}
	}
	tx.activeIterLock.Unlock()
}

// addActiveIter adds the passed iterator to the list of active iterators for
// the pending keys treap.
func (tx *transaction) addActiveIter(iter *treap.Iterator) {
	tx.activeIterLock.Lock()
	tx.activeIters = append(tx.activeIters, iter)
	tx.activeIterLock.Unlock()
}

// notifyActiveIters notifies all of the active iterators for the pending keys
// treap that it has been updated.
func (tx *transaction) notifyActiveIters() {
	tx.activeIterLock.RLock()
	for _, iter := range tx.activeIters {
		iter.ForceReseek()
	}
	tx.activeIterLock.RUnlock()
}

// checkClosed returns an error if the database or transaction is closed.
func (tx *transaction) checkClosed() error {
	// The transaction is no longer valid if it has been closed.
	if tx.closed {
		return makeDbErr(database.ErrTxClosed, errTxClosedStr, nil)
	}

	return nil
}

// hasKey returns whether or not the provided key exists in the database while
// taking into account the current transaction state.
func (tx *transaction) hasKey(key []byte) bool {
	// When the transaction is writable, check the pending transaction
	// state first.
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return false
		}
		if tx.pendingKeys.Has(key) {
			return true
		}
	}

	// Consult the database cache and underlying database.
	return tx.snapshot.Has(key)
}

// putKey adds the provided key to the list of keys to be updated in the
// database when the transaction is committed.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) putKey(key, value []byte) error {
	// Prevent the key from being deleted if it was previously scheduled
	// to be deleted on transaction commit.
	tx.pendingRemove.Delete(key)

	// Add the key/value pair to the list to be written on transaction
	// commit.
	tx.pendingKeys.Put(key, value)
	tx.notifyActiveIters()
	return nil
}

// fetchKey attempts to fetch the provided key from the database cache (and
// hence underlying database) while taking into account the current transaction
// state.  Returns nil if the key does not exist.
func (tx *transaction) fetchKey(key []byte) []byte {
	// When the transaction is writable, check the pending transaction
	// state first.
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return nil
		}
		if value := tx.pendingKeys.Get(key); value != nil {
			return value
		}
	}

	// Consult the database cache and underlying database.
	return tx.snapshot.Get(tx, key)
}

// deleteKey adds the provided key to the list of keys to be deleted from the
// database when the transaction is committed.  The notify iterators flag is
// useful to delay notifying iterators about the changes during bulk deletes.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) deleteKey(key []byte, notifyIterators bool) {
	// Remove the key from the list of pendings keys to be written on
	// transaction commit if needed.
	tx.pendingKeys.Delete(key)

	// Add the key to the list to be deleted on transaction	commit.
	tx.pendingRemove.Put(key, nil)

	// Notify the active iterators about the change if the flag is set.
	if notifyIterators {
		tx.notifyActiveIters()
	}
}

// nextBucketID returns the next bucket ID to use for creating a new bucket.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) nextBucketID() ([4]byte, error) {
	// Load the currently highest used bucket ID.
	curIDBytes := tx.fetchKey(curBucketIDKeyName)
	if curIDBytes == nil {
		curIDBytes = blockIdxBucketID[:]
	}
	curBucketNum := binary.BigEndian.Uint32(curIDBytes)

	// Increment and update the current bucket ID and return it.
	var nextBucketID [4]byte
	binary.BigEndian.PutUint32(nextBucketID[:], curBucketNum+1)
	if err := tx.putKey(curBucketIDKeyName, nextBucketID[:]); err != nil {
		return [4]byte{}, err
	}
	return nextBucketID, nil
}

// Metadata returns the top-most bucket for all metadata storage.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) Metadata() database.Bucket {
	return tx.metaBucket
}

// hasBlock returns whether or not a block with the given hash exists.
func (tx *transaction) hasBlock(hash *chainhash.Hash) bool {
	// Return true if the block is pending to be written on commit since
	// it exists from the viewpoint of this transaction.
	if _, exists := tx.pendingBlocks[*hash]; exists {
		return true
	}

	return tx.hasKey(bucketizedKey(blockIdxBucketID, hash[:]))
}

// StoreBlock stores the provided block into the database.  There are no checks
// to ensure the block connects to a previous block, contains double spends, or
// any additional functionality such as transaction indexing.  It simply stores
// the block in the database.
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockExists when the block hash already exists
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) StoreBlock(block *btcutil.Block) error {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !tx.writable {
		str := "store block requires a writable database transaction"
		return makeDbErr(database.ErrTxNotWritable, str, nil)
	}

	// Reject the block if it already exists.
	blockHash := block.Hash()
	if tx.hasBlock(blockHash) {
		str := fmt.Sprintf("block %s already exists", blockHash)
		return makeDbErr(database.ErrBlockExists, str, nil)
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		str := fmt.Sprintf("failed to get serialized bytes for block %s", blockHash)
		return makeDbErr(database.ErrDriverSpecific, str, err)
	}

	// Add the block to be stored to the list of pending blocks to store
	// when the transaction is committed.  Also, add it to pending blocks
	// map so it is easy to determine the block is pending based on the
	// block hash.
	if tx.pendingBlocks == nil {
		tx.pendingBlocks = make(map[chainhash.Hash]int)
	}
	tx.pendingBlocks[*blockHash] = len(tx.pendingBlockData)
	tx.pendingBlockData = append(tx.pendingBlockData, pendingBlock{
		hash:   blockHash,
		bytes:  blockBytes,
		height: block.Height(),
	})
	log.Tracef("Added block %s to pending blocks", blockHash)

	return nil
}

// HasBlock returns whether or not a block with the given hash exists in the
// database.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) HasBlock(hash *chainhash.Hash) (bool, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return false, err
	}

	return tx.hasBlock(hash), nil
}

// HasBlocks returns whether or not the blocks with the provided hashes
// exist in the database.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) HasBlocks(hashes []chainhash.Hash) ([]bool, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	results := make([]bool, len(hashes))
	for i := range hashes {
		results[i] = tx.hasBlock(&hashes[i])
	}

	return results, nil
}

// fetchBlockRow fetches the metadata stored in the block index for the provided
// hash.  It will return ErrBlockNotFound if there is no entry.
func (tx *transaction) fetchBlockRow(hash *chainhash.Hash) ([]byte, error) {
	blockRow := tx.blockIdxBucket.Get(hash[:])
	if blockRow == nil {
		str := fmt.Sprintf("block %s does not exist", hash)
		return nil, makeDbErr(database.ErrBlockNotFound, str, nil)
	}

	return blockRow, nil
}

// FetchBlockHeader returns the raw serialized bytes for the block header
// identified by the given hash.  The raw bytes are in the format returned by
// Serialize on a wire.BlockHeader.
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockNotFound if the requested block hash does not exist
//   - ErrTxClosed if the transaction has already been closed
//   - ErrCorruption if the database has somehow become corrupted
//
// NOTE: The data returned by this function is only valid during a
// database transaction.  Attempting to access it after a transaction
// has ended results in undefined behavior.  This constraint prevents
// additional data copies and allows support for memory-mapped database
// implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlockHeader(hash *chainhash.Hash) ([]byte, error) {
	return tx.FetchBlockRegion(&database.BlockRegion{
		Hash:   hash,
		Offset: 0,
		Len:    blockHdrSize,
	})
}

// FetchBlockHeaders returns the raw serialized bytes for the block headers
// identified by the given hashes.  The raw bytes are in the format returned by
// Serialize on a wire.BlockHeader.
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockNotFound if the any of the requested block hashes do not exist
//   - ErrTxClosed if the transaction has already been closed
//   - ErrCorruption if the database has somehow become corrupted
//
// NOTE: The data returned by this function is only valid during a database
// transaction.  Attempting to access it after a transaction has ended results
// in undefined behavior.  This constraint prevents additional data copies and
// allows support for memory-mapped database implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlockHeaders(hashes []chainhash.Hash) ([][]byte, error) {
	regions := make([]database.BlockRegion, len(hashes))
	for i := range hashes {
		regions[i].Hash = &hashes[i]
		regions[i].Offset = 0
		regions[i].Len = blockHdrSize
	}
	return tx.FetchBlockRegions(regions)
}

// FetchBlock returns the raw serialized bytes for the block identified by the
// given hash.  The raw bytes are in the format returned by Serialize on a
// wire.MsgBlock.
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockNotFound if the requested block hash does not exist
//   - ErrTxClosed if the transaction has already been closed
//   - ErrCorruption if the database has somehow become corrupted
//
// In addition, returns ErrDriverSpecific if any failures occur when reading the
// block files.
//
// NOTE: The data returned by this function is only valid during a database
// transaction.  Attempting to access it after a transaction has ended results
// in undefined behavior.  This constraint prevents additional data copies and
// allows support for memory-mapped database implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlock(hash *chainhash.Hash) ([]byte, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// When the block is pending to be written on commit return the bytes
	// from there.
	if idx, exists := tx.pendingBlocks[*hash]; exists {
		return tx.pendingBlockData[idx].bytes, nil
	}

	// Lookup the location of the block in the files from the block index.
	blockRow, err := tx.fetchBlockRow(hash)
	if err != nil {
		return nil, err
	}
	location, err := deserializeBlockLoc(blockRow)
	if err != nil {
		return nil, err
	}

	// Read the block from the appropriate location.  The function also
	// performs a checksum over the data to detect data corruption.
	blockBytes, err := tx.db.store.readBlock(hash, *location)
	if err != nil {
		return nil, err
	}

	return blockBytes, nil
}

// FetchBlocks returns the raw serialized bytes for the blocks identified by the
// given hashes.  The raw bytes are in the format returned by Serialize on a
// wire.MsgBlock.
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockNotFound if any of the requested block hashed do not exist
//   - ErrTxClosed if the transaction has already been closed
//   - ErrCorruption if the database has somehow become corrupted
//
// In addition, returns ErrDriverSpecific if any failures occur when reading the
// block files.
//
// NOTE: The data returned by this function is only valid during a database
// transaction.  Attempting to access it after a transaction has ended results
// in undefined behavior.  This constraint prevents additional data copies and
// allows support for memory-mapped database implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlocks(hashes []chainhash.Hash) ([][]byte, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// NOTE: This could check for the existence of all blocks before loading
	// any of them which would be faster in the failure case, however
	// callers will not typically be calling this function with invalid
	// values, so optimize for the common case.

	// Load the blocks.
	blocks := make([][]byte, len(hashes))
	for i := range hashes {
		var err error
		blocks[i], err = tx.FetchBlock(&hashes[i])
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

// fetchPendingRegion attempts to fetch the provided region from any block which
// are pending to be written on commit.  It will return nil for the byte slice
// when the region references a block which is not pending.  When the region
// does reference a pending block, it is bounds checked and returns
// ErrBlockRegionInvalid if invalid.
func (tx *transaction) fetchPendingRegion(region *database.BlockRegion) ([]byte, error) {
	// Nothing to do if the block is not pending to be written on commit.
	idx, exists := tx.pendingBlocks[*region.Hash]
	if !exists {
		return nil, nil
	}

	// Ensure the region is within the bounds of the block.
	blockBytes := tx.pendingBlockData[idx].bytes
	blockLen := uint32(len(blockBytes))
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > blockLen {
		str := fmt.Sprintf("block %s region offset %d, length %d "+
			"exceeds block length of %d", region.Hash,
			region.Offset, region.Len, blockLen)
		return nil, makeDbErr(database.ErrBlockRegionInvalid, str, nil)
	}

	// Return the bytes from the pending block.
	return blockBytes[region.Offset:endOffset:endOffset], nil
}

// FetchBlockRegion returns the raw serialized bytes for the given block region.
//
// For example, it is possible to directly extract Bitcoin transactions and/or
// scripts from a block with this function.  Depending on the backend
// implementation, this can provide significant savings by avoiding the need to
// load entire blocks.
//
// The raw bytes are in the format returned by Serialize on a wire.MsgBlock and
// the Offset field in the provided BlockRegion is zero-based and relative to
// the start of the block (byte 0).
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockNotFound if the requested block hash does not exist
//   - ErrBlockRegionInvalid if the region exceeds the bounds of the associated
//     block
//   - ErrTxClosed if the transaction has already been closed
//   - ErrCorruption if the database has somehow become corrupted
//
// In addition, returns ErrDriverSpecific if any failures occur when reading the
// block files.
//
// NOTE: The data returned by this function is only valid during a database
// transaction.  Attempting to access it after a transaction has ended results
// in undefined behavior.  This constraint prevents additional data copies and
// allows support for memory-mapped database implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlockRegion(region *database.BlockRegion) ([]byte, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// When the block is pending to be written on commit return the bytes
	// from there.
	if tx.pendingBlocks != nil {
		regionBytes, err := tx.fetchPendingRegion(region)
		if err != nil {
			return nil, err
		}
		if regionBytes != nil {
			return regionBytes, nil
		}
	}

	// Lookup the location of the block in the files from the block index.
	blockRow, err := tx.fetchBlockRow(region.Hash)
	if err != nil {
		return nil, err
	}
	location, err := deserializeBlockLoc(blockRow)
	if err != nil {
		str := fmt.Sprintf("no data for: %s ", region.Hash)
		return nil, makeDbErr(database.ErrBlockRegionInvalid, str, err)
	}

	// Ensure the region is within the bounds of the block.
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > location.blockLen {
		str := fmt.Sprintf("block %s region offset %d, length %d "+
			"exceeds block length of %d", region.Hash,
			region.Offset, region.Len, location.blockLen)
		return nil, makeDbErr(database.ErrBlockRegionInvalid, str, nil)

	}

	// Read the region from the appropriate disk block file.
	regionBytes, err := tx.db.store.readBlockRegion(*location, region.Offset,
		region.Len)
	if err != nil {
		return nil, err
	}

	return regionBytes, nil
}

// FetchBlockRegions returns the raw serialized bytes for the given block
// regions.
//
// For example, it is possible to directly extract Bitcoin transactions and/or
// scripts from various blocks with this function.  Depending on the backend
// implementation, this can provide significant savings by avoiding the need to
// load entire blocks.
//
// The raw bytes are in the format returned by Serialize on a wire.MsgBlock and
// the Offset fields in the provided BlockRegions are zero-based and relative to
// the start of the block (byte 0).
//
// Returns the following errors as required by the interface contract:
//   - ErrBlockNotFound if any of the request block hashes do not exist
//   - ErrBlockRegionInvalid if one or more region exceed the bounds of the
//     associated block
//   - ErrTxClosed if the transaction has already been closed
//   - ErrCorruption if the database has somehow become corrupted
//
// In addition, returns ErrDriverSpecific if any failures occur when reading the
// block files.
//
// NOTE: The data returned by this function is only valid during a database
// transaction.  Attempting to access it after a transaction has ended results
// in undefined behavior.  This constraint prevents additional data copies and
// allows support for memory-mapped database implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlockRegions(regions []database.BlockRegion) ([][]byte, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// NOTE: This could check for the existence of all blocks before
	// deserializing the locations and building up the fetch list which
	// would be faster in the failure case, however callers will not
	// typically be calling this function with invalid values, so optimize
	// for the common case.

	// NOTE: A potential optimization here would be to combine adjacent
	// regions to reduce the number of reads.

	// In order to improve efficiency of loading the bulk data, first grab
	// the block location for all of the requested block hashes and sort
	// the reads by filenum:offset so that all reads are grouped by file
	// and linear within each file.  This can result in quite a significant
	// performance increase depending on how spread out the requested hashes
	// are by reducing the number of file open/closes and random accesses
	// needed.  The fetchList is intentionally allocated with a cap because
	// some of the regions might be fetched from the pending blocks and
	// hence there is no need to fetch those from disk.
	blockRegions := make([][]byte, len(regions))
	fetchList := make([]bulkFetchData, 0, len(regions))
	for i := range regions {
		region := &regions[i]

		// When the block is pending to be written on commit grab the
		// bytes from there.
		if tx.pendingBlocks != nil {
			regionBytes, err := tx.fetchPendingRegion(region)
			if err != nil {
				return nil, err
			}
			if regionBytes != nil {
				blockRegions[i] = regionBytes
				continue
			}
		}

		// Lookup the location of the block in the files from the block
		// index.
		blockRow, err := tx.fetchBlockRow(region.Hash)
		if err != nil {
			return nil, err
		}
		location, err := deserializeBlockLoc(blockRow)
		if err != nil {
			return nil, err
		}

		// Ensure the region is within the bounds of the block.
		endOffset := region.Offset + region.Len
		if endOffset < region.Offset || endOffset > location.blockLen {
			str := fmt.Sprintf("block %s region offset %d, length "+
				"%d exceeds block length of %d", region.Hash,
				region.Offset, region.Len, location.blockLen)
			return nil, makeDbErr(database.ErrBlockRegionInvalid, str, nil)
		}

		fetchList = append(fetchList, bulkFetchData{location, i})
	}
	sort.Sort(bulkFetchDataSorter(fetchList))

	// Read all of the regions in the fetch list and set the results.
	for i := range fetchList {
		fetchData := &fetchList[i]
		ri := fetchData.replyIndex
		region := &regions[ri]
		location := fetchData.blockLocation
		regionBytes, err := tx.db.store.readBlockRegion(*location, region.Offset, region.Len)
		if err != nil {
			return nil, err
		}
		blockRegions[ri] = regionBytes
	}

	return blockRegions, nil
}

// close marks the transaction closed then releases any pending data, the
// underlying snapshot, the transaction read lock, and the write lock when the
// transaction is writable.
func (tx *transaction) close() {
	tx.closed = true

	// Clear pending blocks that would have been written on commit.
	tx.pendingBlocks = nil
	tx.pendingBlockData = nil

	// Clear pending file deletions.
	tx.pendingDelFileNums = nil

	// Clear pending keys that would have been written or deleted on commit.
	tx.pendingKeys = nil
	tx.pendingRemove = nil

	tx.closeMdbTxs()
	// Release the snapshot.
	if tx.snapshot != nil {
		tx.snapshot.Release()
		tx.snapshot = nil
	}

	tx.db.closeLock.RUnlock()

	// Release the writer lock for writable transactions to unblock any
	// other write transaction which are possibly waiting.
	if tx.writable {
		// fmt.Println("------------ AndyDbgMsg: unlock:", tx.db.getNum)
		tx.db.writeLock.Unlock()
	}
}

// writePendingAndCommit writes pending block data to the flat block files,
// updates the metadata with their locations as well as the new current write
// location, and commits the metadata to the memory database cache.  It also
// properly handles rollback in the case of failures.
//
// This function MUST only be called when there is pending data to be written.
func (tx *transaction) writePendingAndCommit() error {
	// Loop through all the pending file deletions and delete them.
	// We do this first before doing any of the writes as we can't undo
	// deletions of files.
	for _, fileNum := range tx.pendingDelFileNums {
		err := tx.db.store.deleteFileFunc(fileNum)
		if err != nil {
			// Nothing we can do if we fail to delete blocks besides
			// return an error.
			return err
		}
	}

	// Save the current block store write position for potential rollback.
	// These variables are only updated here in this function and there can
	// only be one write transaction active at a time, so it's safe to store
	// them for potential rollback.
	wc := tx.db.store.writeCursor
	wc.RLock()
	oldBlkFileNum := wc.curFileNum
	oldBlkOffset := wc.curOffset
	wc.RUnlock()

	// rollback is a closure that is used to rollback all writes to the
	// block files.
	rollback := func() {
		// Rollback any modifications made to the block files if needed.
		tx.db.store.handleRollback(oldBlkFileNum, oldBlkOffset)
	}

	// Loop through all of the pending blocks to store and write them.
	for _, blockData := range tx.pendingBlockData {
		log.Tracef("Storing block %s", blockData.hash)
		location, err := tx.db.store.writeBlock(tx, blockData.height, blockData.bytes)
		if err != nil {
			rollback()
			return err
		}

		// Add a record in the block index for the block.  The record
		// includes the location information needed to locate the block
		// on the filesystem as well as the block header since they are
		// so commonly needed.
		blockRow := serializeBlockLoc(location)
		err = tx.blockIdxBucket.Put(blockData.hash[:], blockRow)
		if err != nil {
			rollback()
			return err
		}
	}

	// Update the metadata for the current write file and offset.
	writeRow := serializeWriteRow(wc.curFileNum, wc.curOffset)
	if err := tx.metaBucket.Put(writeLocKeyName, writeRow); err != nil {
		rollback()
		return convertErr("failed to store write cursor", err)
	}

	// Atomically update the database cache.  The cache automatically
	// handles flushing to the underlying persistent storage database.
	return tx.db.cache.commitTx(tx)
}

// PruneBlocks deletes the block files until it reaches the target size
// (specified in bytes).  Throws an error if the target size is below
// the maximum size of a single block file.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) PruneBlocks(targetSize uint64) ([]chainhash.Hash, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !tx.writable {
		str := "prune blocks requires a writable database transaction"
		return nil, makeDbErr(database.ErrTxNotWritable, str, nil)
	}

	// Make a local alias for the maxBlockFileSize.
	maxSize := uint64(tx.db.store.maxBlockFileSize)
	if targetSize < maxSize {
		return nil, fmt.Errorf("got target size of %d but it must be greater "+
			"than %d, the max size of a single block file",
			targetSize, maxSize)
	}

	first, last, lastFileSize, err := scanBlockFiles(tx.db.store.basePath)
	if err != nil {
		return nil, err
	}

	// If we have no files on disk or just a single file on disk, return early.
	if first == last {
		return nil, nil
	}

	// Last file number minus the first file number gives us the count of files
	// on disk minus 1.  We don't want to count the last file since we can't assume
	// that it is of max size.
	maxSizeFileCount := last - first

	// If the total size of block files are under the target, return early and
	// don't prune.
	totalSize := uint64(lastFileSize) + (maxSize * uint64(maxSizeFileCount))
	if totalSize <= targetSize {
		return nil, nil
	}

	log.Tracef("Using %d more bytes than the target of %d MiB. Pruning files...",
		totalSize-targetSize,
		targetSize/(1024*1024))

	deletedFiles := make(map[uint32]struct{})

	// We use < not <= so that the last file is never deleted.  There are other checks in place
	// but setting it to < here doesn't hurt.
	for i := uint32(first); i < uint32(last); i++ {
		// Add the block file to be deleted to the list of files pending deletion to
		// delete when the transaction is committed.
		if tx.pendingDelFileNums == nil {
			tx.pendingDelFileNums = make([]uint32, 0, 1)
		}
		tx.pendingDelFileNums = append(tx.pendingDelFileNums, i)

		// Add the file index to the deleted files map so that we can later
		// delete the block location index.
		deletedFiles[i] = struct{}{}

		// If we're already at or below the target usage, break and don't
		// try to delete more files.
		totalSize -= maxSize
		if totalSize <= targetSize {
			break
		}
	}

	// Delete the indexed block locations for the files that we've just deleted.
	var deletedBlockHashes []chainhash.Hash
	cursor := tx.blockIdxBucket.Cursor()
	for ok := cursor.First(); ok; ok = cursor.Next() {
		loc, err := deserializeBlockLoc(cursor.Value())
		if err != nil {
			return nil, err
		}

		_, found := deletedFiles[loc.blockFileNum]
		if found {
			deletedBlockHashes = append(deletedBlockHashes, *(*chainhash.Hash)(cursor.Key()))
			err := cursor.Delete()
			if err != nil {
				return nil, err
			}
		}
	}

	log.Tracef("Finished pruning. Database now at %d bytes", totalSize)

	return deletedBlockHashes, nil
}

// BeenPruned returns if the block storage has ever been pruned.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) BeenPruned() (bool, error) {
	first, last, _, err := scanBlockFiles(tx.db.store.basePath)
	if err != nil {
		return false, err
	}

	// If the database is pruned, then the first .fdb will not be there.
	// We also check that there isn't just 1 file on disk or if there are
	// no files on disk by checking if first != last.
	return first != 0 && (first != last), nil
}

// Commit commits all changes that have been made to the root metadata bucket
// and all of its sub-buckets to the database cache which is periodically synced
// to persistent storage.  In addition, it commits all new blocks directly to
// persistent storage bypassing the db cache.  Blocks can be rather large, so
// this help increase the amount of cache available for the metadata updates and
// is safe since blocks are immutable.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) Commit() error {
	// Prevent commits on managed transactions.
	if tx.managed {
		tx.close()
		panic("managed transaction commit not allowed")
	}

	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	// Regardless of whether the commit succeeds, the transaction is closed
	// on return.
	defer tx.close()

	// Ensure the transaction is writable.
	if !tx.writable {
		str := "Commit requires a writable database transaction"
		return makeDbErr(database.ErrTxNotWritable, str, nil)
	}

	// Write pending data.  The function will rollback if any errors occur.
	err := tx.writePendingAndCommit()
	if err != nil {
		return err
	}
	if tx.mdbRwTx != nil {
		err := tx.mdbRwTx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

// Rollback undoes all changes that have been made to the root bucket and all of
// its sub-buckets.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) Rollback() error {
	// Prevent rollbacks on managed transactions.
	if tx.managed {
		tx.close()
		panic("managed transaction rollback not allowed")
	}

	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	tx.close()
	return nil
}

func (tx *transaction) closeMdbTxs() {
	if tx.mdbRoTx != nil {
		tx.mdbRoTx.Rollback()
		tx.mdbRoTx = nil
	}
	if tx.mdbRwTx != nil {
		tx.mdbRwTx.Rollback()
		tx.mdbRwTx = nil
	}
}
