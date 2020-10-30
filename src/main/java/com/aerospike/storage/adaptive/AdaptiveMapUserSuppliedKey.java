/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.storage.adaptive;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PredExp;

/**
 * This class is designed to overcome record size limits of traditional maps on Aerospike. Consider storing time-series
 * data, such as credit card transactions associated with a particular customer. The number of transactions are unbounded
 * but Aerospike imposes finite size records limits; <= 1MB in earlier than 4.2, up to 8MB in 4.2+. In this example for
 * example we want to store a map which is keyed by timestamp and the data is the appropriate credit card transaction.
 * <p>
 * If it is assumed that credit card transaction is 1kB in size, this means it is not possible to store more than say
 * 8,000 transactions in a single map. Whilst there are strategies around this (for example by storing the transactions
 * on a per-day basis and querying multiple records) they all suffer from particular drawbacks, not the least of which
 * is the increase in code complexity.
 * <p>
 *
 * @author Tim
 *
 */
@SuppressWarnings("serial")
public class AdaptiveMapUserSuppliedKey implements IAdaptiveMap  {
	/** The value to put in the map when the record is locked */
	private static final String LOCK_MAP_ENTRY = "locked";
	/**
	 * When the record in unlocked we need a map entry which is exactly the same size as the locked entry. This effectively
	 * reserves space in the record -- if we wish to split on RECORD_TOO_BIG exceptions, there is a possibility that the
	 * underlying record will not have space to hold a lock value, forcing an unsplittable block.
	 * <p>
	 * For example, consider a record which is 5 bytes under the split limit. If a 1kB write comes in, it will force the
	 * record to split, which will need to obtain the lock. If there is no space reserved for the lock, this lock will fail
	 * again with RECORD_TOO_BIG which means no more items can be inserted into this record.
	 */
	private static final String UNLOCK_MAP_ENTRY = "unlckd";

	/** The name of the bin in which to store data */
	private final String dataBinName;

	/**
	 * The name of the bin used to lock a record. This bin will end up containing a map: if the map contains certain
	 * key the lock is held, if the map is empty or does not exist, the lock is not held.
	 */
	private static final String LOCK_BIN = "lock";

	/**
	 * The name of the bin on the root block used to hold the block bitmap. The bitmap determines if a particular block has
	 * been split -- a 1 in the bitmap for the block says that the block has split.
	 */
	private static final String BLOCK_MAP_BIN = "blks";
	private static final String ROOT_INDICATOR = "root_blk";

	/**
	 * The maximum time any lock should be held for during the splitting of a record
	 */
	private static final long MAX_LOCK_TIME = 100;
	/**
	 * A moderately unique ID
	 */
	private static final String ID = UUID.randomUUID().toString(); // This needs to be unique only for this session, used only for locking
	private static final long HASH_ID = computeHash(ID);
	private final boolean sendKey = true;
	private final IAerospikeClient client;
	private final String namespace;
	private final String setName;

	private final MapPolicy mapPolicy;
	private final int recordThreshold;
	private final boolean forceDurableDeletes;

	private static long computeHash(String string) {
		long h = 1125899906842597L; // prime
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}

	/**
	 * Turn a set of data stored in the Adaptive map into an object. Used in the <code>get</code> methods to form the results
	 * directly into their desired form, instead of returning a generic form which must then be manually transformed.
	 * @author timfaulkes
	 *
	 * @param <T>
	 */
	public interface ObjectMapper<T> {
		public T map(String recordKey, long mapKey, Object object);
	}

	/**
	 * Create a new adaptive map
	 *
	 * @param client - the AerospikeClient used to manipulate this map
	 * @param namespace - The namespace for the map
	 * @param setName - The set name to use for the map
	 * @param mapBin - The bin name to store the bin in. Note that this class reserves the right to create other bins on the record
	 * to store things like lock information
	 * @param mapPolicy - The mapPolicy to use when inserting an element into the map
	 * @param recordThreshold - How many records can be stored in a block before the block splits. For example, if this number is set to
	 * 1,000, the 1,001st record would cause the block to split on inserting. It is possible that the block will split before this however --
	 * if the insertion of a map entry results in a RECORD_TOO_BIG exception, the block will split automatically if there is > 1 entry.
	 * @param forceDurableDeletes - If true, all deletes will be done durably irrespective of the durableDelete flag passed in the write policy. If false, the writePolicy will be honored.
	 * For strong consistency namespaces with strong-consistency-allow-expunges set to false, this flag should be true.
	 */
	public AdaptiveMapUserSuppliedKey(final IAerospikeClient client, final String namespace, final String setName, final String mapBin, final MapPolicy mapPolicy, int recordThreshold, boolean forceDurableDeletes) {
		this.client = client;
		this.setName = setName;
		this.namespace = namespace;
		this.mapPolicy = mapPolicy == null ? new MapPolicy(MapOrder.KEY_ORDERED, 0) : mapPolicy;
		this.recordThreshold = recordThreshold;
		if (mapBin == null || mapBin.isEmpty()) {
			throw new IllegalArgumentException("mapBin must be specified");
		}
		else if (LOCK_BIN.equals(mapBin) || BLOCK_MAP_BIN.equals(mapBin)) {
			throw new IllegalArgumentException("mapBin cannot be either " + LOCK_BIN + " or " + BLOCK_MAP_BIN);
		}
		this.dataBinName = mapBin;
		this.forceDurableDeletes = forceDurableDeletes;
	}

	/**
	 * Create a new adaptive map.
	 * <p/>
	 * Note that this constructor has to determine whether to force durable deletes and so it needs to connect to the cluster and use the Info protocol.
	 * Hence, if this constructor is used, ensure the connected user has priviledges to do this.
	 *
	 * @param client - the AerospikeClient used to manipulate this map
	 * @param namespace - The namespace for the map
	 * @param setName - The set name to use for the map
	 * @param mapBin - The bin name to store the bin in. Note that this class reserves the right to create other bins on the record
	 * to store things like lock information
	 * @param mapPolicy - The mapPolicy to use when inserting an element into the map
	 * @param recordThreshold - How many records can be stored in a block before the block splits. For example, if this number is set to
	 * 1,000, the 1,001st record would cause the block to split on inserting. It is possible that the block will split before this however --
	 * if the insertion of a map entry results in a RECORD_TOO_BIG exception, the block will split automatically if there is > 1 entry.
	 */
	public AdaptiveMapUserSuppliedKey(final IAerospikeClient client, final String namespace, final String setName, final String mapBin, final MapPolicy mapPolicy, int recordThreshold) {
		// We need to determine if durable deletes must be done irrespective of the write policy, so look at the namespace info for this.
		this(client, namespace, setName, mapBin, mapPolicy, recordThreshold, forceDurableDeletes(client, namespace));
	}

	@Override
	public void put(WritePolicy writePolicy, String recordKey, Object mapKey, byte[] mapKeyDigest, Value value) {
		if (mapKey instanceof Long || mapKey instanceof Integer || mapKey instanceof Short || mapKey instanceof Byte) {
			put(writePolicy, recordKey, ((Number)mapKey).longValue(), value);
		}
		else {
			throw new java.lang.UnsupportedOperationException("Method not implemented.");
		}
	}

	private static boolean forceDurableDeletes(IAerospikeClient client, String namespace) {
		Node[] nodes = client.getNodes();
		if (nodes.length == 0) {
			throw new IllegalArgumentException("Could not connect to the cluster");
		}
		String[] config = Info.request(client.getNodes()[0], "namespace/"+namespace).split(";");
		boolean isSC = false;
		boolean allowExpunge = false;
		for (String thisConfig : config) {
			if (thisConfig.startsWith("strong-consistency")) {
				String[] nameAndValue = thisConfig.split("=");
				String name = nameAndValue[0];
				String value = nameAndValue[1];
				if ("strong-consistency".equals(name)) {
					isSC = Boolean.valueOf(value);
				}
				else if ("strong-consistency-allow-expunge".equals(name)) {
					allowExpunge = Boolean.valueOf(value);
				}
			}
		}
		return isSC && (!allowExpunge);
	}

	/**
	 * Given a key and the map entries looked up from that key (IndexRelativeRange(-1, 3), determine which
	 * block this key falls into.
	 * @param mapKey
	 * @param mapEntries
	 * @return
	 */
	private long computeBlockNumber(long mapKey, List<Entry<Long, Long>> mapEntries) {
		// The array will contain either 1,2 or 3 values. We want the largest value <= mapKey
		// NOTE: For this to work all the time the lowest block must have a map entry of Long. MIN_VALUE
		Entry<Long, Long> bestEntry = null;
		for (Entry<Long, Long> entry : mapEntries) {
			if (entry.getKey() == mapKey) {
				// exact match, this is the one we want
				return entry.getValue();
			}
			else if (entry.getKey() < mapKey && (bestEntry == null || entry.getKey() > bestEntry.getKey())) {
				bestEntry = entry;
			}
		}
		return bestEntry.getValue();
	}

	/**
	 * Given a key and the map entries looked up from that key (IndexRelativeRange(-1, 3), determine if
	 * this block is the last block or now.
	 * @param mapKey
	 * @param mapEntries
	 * @return
	 */
	private boolean isLastBlock(long mapKey, List<Entry<Long, Long>> mapEntries) {
		// If the last entry is the one which should contain our key, this is the last block. (Given
		// we asked for our block, the one before and the one after)
		if (mapEntries == null || mapEntries.isEmpty()) {
			return true;
		}
		Entry<Long, Long> bestEntry = mapEntries.get(mapEntries.size() - 1);
		return (bestEntry.getKey() <= mapKey);
	}

	/**
	 * Generate a unique-ish number. This will be the block number per key, so should have a large amount
	 * of entropy. However, even with 1,000,000 sub blocks out of 1.844x10^19 keys the probability of
	 * 2 colliding is minute.
	 * @return
	 */
	private long generateUniqueNumber() {
		long result =  HASH_ID ^ ThreadLocalRandom.current().nextLong()^Thread.currentThread().getId();
		if (result == 0) {
			return generateUniqueNumber();
		}
		else {
			return result;
		}
	}

	/**
	 * Given the top level record key and the block which we want to load, return a Key object which can be used in Aerospike calls
	 * @param recordKey
	 * @param id
	 * @return
	 */
	private Key getCombinedKey(String recordKey, long id) {
		return new Key(namespace, setName, id != 0 ? recordKey + ":" + id : recordKey);
	}

	private Operation getRelevantBlocksOperation(long id) {
		return MapOperation.getByKeyRelativeIndexRange(BLOCK_MAP_BIN, Value.get(id), -1, 3, MapReturnType.KEY_VALUE);
	}

	/**
	 * Read a single key from the Map and return the value associated with it
	 * @param recordKeyValue
	 * @param mapKey
	 * @param operation
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Object get(String recordKeyValue, long mapKey) {
		Key rootKey = getCombinedKey(recordKeyValue, 0);
		Value mapKeyValue = Value.get(mapKey);
		Record record = client.operate(null, rootKey,
				MapOperation.getByKey(dataBinName, mapKeyValue, MapReturnType.VALUE),
				getRelevantBlocksOperation(mapKey));

		// Check to see if which block we should read
		List<Entry<Long, Long>> minBlocks = (List<Entry<Long, Long>>) record.getValue(BLOCK_MAP_BIN);
		if (minBlocks != null && minBlocks.size() == 0) {
			long block = computeBlockNumber(mapKey, minBlocks);
			record = client.operate(null, getCombinedKey(recordKeyValue, block),
					MapOperation.getByKey(dataBinName, mapKeyValue, MapReturnType.VALUE));
		}
		if (record == null) {
			return null;
		}
		else {
			return record.getValue(dataBinName);
		}
	}

	@Override
	public Object get(String recordKeyValue, int mapKey) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public Object get(String recordKeyValue, String mapKey) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public Object get(String recordKeyValue, byte[] digest) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}


	/**
	 * Get all of the records associated with the passed keyValue. The result will be a TreeMap (ordered map by key) which contains all the records in the adaptive map.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> getAll(Policy readPolicy, String keyValue, ObjectMapper<T> mapper) {
		if (readPolicy == null) {
			readPolicy = new Policy();
		}
		Key rootKey = getCombinedKey(keyValue, 0);
		Set<Record> records = new HashSet<>();
		Record result = client.get(readPolicy, rootKey);

		if (result != null) {
			List<T> objectResults = new ArrayList<T>(records.size());

			TreeMap<Long, Long> blockMap = (TreeMap<Long, Long>) result.getValue(BLOCK_MAP_BIN);
			if (blockMap != null && !blockMap.isEmpty()) {

				BatchPolicy batchPolicy = new BatchPolicy();
				batchPolicy.maxRetries = readPolicy.maxRetries;
				batchPolicy.sleepBetweenRetries = readPolicy.sleepBetweenRetries;
				batchPolicy.timeoutDelay = readPolicy.timeoutDelay;
				batchPolicy.socketTimeout = readPolicy.socketTimeout;
				batchPolicy.totalTimeout = readPolicy.totalTimeout;

				batchPolicy.maxConcurrentThreads = 0;

				// This block has been split, the results are in the sub-blocks
				Map<Long, Record> batchRecords = new HashMap<>();
				List<Long> blockList = new ArrayList<>(blockMap.values());
				Set<Long> blocksInDoubt = new HashSet<>();
				while (blockList.size() > 0)  {
					// The blocks to be read are now in blockList. This give an index
					Key[] keys = new Key[blockList.size()];
					long[] rawKeys = new long[blockList.size()];
					int count = 0;
					for (Long thisBlock : blockList) {
						rawKeys[count] = thisBlock;
						keys[count++] = getCombinedKey(keyValue, thisBlock);
					}
					Record[] results = client.get(batchPolicy, keys);
					for (count = 0; count < results.length; count++) {
						if (results[count] == null) {
							// This has potentially split
							blocksInDoubt.add(rawKeys[count]);
						}
						else {
							batchRecords.put(rawKeys[count], results[count]);
						}
					}

					blockList.clear();
					if (blocksInDoubt.size() > 0) {
						// Re-read the root record to determine what's happened. This should be very rare: (a block TTLd out, or a block split
						// between the time we read the original bitmap and when we read this record)
						Key key = new Key(namespace, setName, keyValue);
						Record rootRecord = client.get(batchPolicy, key, BLOCK_MAP_BIN);
						if (rootRecord != null) {
							TreeMap<Long, Long> newBlockMap = (TreeMap<Long, Long>) result.getValue(BLOCK_MAP_BIN);
							// Iterate through all the blocks to see which ones we have not retrieved
							Collection<Long> retrievedBlockIds = newBlockMap.values();
							for (long thisId : retrievedBlockIds) {
								if (!batchRecords.containsKey(thisId)) {
									blockList.add(thisId);
								}
							}
							blockMap = newBlockMap;
						}
					}
				}

				// To get here, we must have a block list which has all the blocks in, amalgamate them
				for (long blockMin : blockMap.keySet()) {
					long thisBlock = blockMap.get(blockMin);
					Record record = batchRecords.get(thisBlock);
					TreeMap<Long, Object> map = (TreeMap<Long, Object>) record.getMap(dataBinName);
					for (long key : map.keySet()) {
						objectResults.add( mapper.map(keyValue, key, map.get(key)));
					}
				}
			}
			else {
				// This block exists and has not been split, therefore it contains the results.
				TreeMap<Long, Object> map = (TreeMap<Long, Object>) result.getMap(dataBinName);
				for (long key : map.keySet()) {
					objectResults.add( mapper.map(keyValue, key, map.get(key)));
				}
			}
			return objectResults;
		}
		else {
			return null;
		}
	}

	@Override
	public TreeMap<Object, Object> getAll(Policy readPolicy, String keyValue) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public TreeMap<Object, Object>[] getAll(BatchPolicy batchPolicy, String []keyValues) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public TreeMap<Object, Object>[] getAll(BatchPolicy batchPolicy, String[] recordKeyValues, int filterCount) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}

	/**
	 * Get a count of all of the records associated with the passed keyValue.
	 */
	@Override
	//@Override
	@SuppressWarnings("unchecked")
	public int countAll(WritePolicy policy, String keyValue) {
		// TODO: refactor this to the Async API.
		Key rootKey = new Key(namespace, setName, keyValue);
		Record result = client.operate(policy, rootKey, Operation.get(BLOCK_MAP_BIN), MapOperation.getByIndexRange(this.dataBinName, 0, MapReturnType.COUNT));

		if (result != null) {
			Map<Long, Long> map = (Map<Long, Long>) result.getValue(BLOCK_MAP_BIN);
			if (map == null || map.isEmpty()) {
				// This block exists and has not been split, therefore it contains the results.
				return result.getInt(this.dataBinName);
			}
			else {
				// This block has been split, the results are in the sub-blocks
				Collection<Long> blockList = map.values();
				boolean blocksInDoubt;
				Map<Long, Long> blockCounts = new HashMap<>();

				while (blockList.size() > 0)  {
					blocksInDoubt = false;
					for (long thisBlock : blockList) {
						if (!blockCounts.containsKey(thisBlock)) {
							Record mapCount = client.operate(null, getCombinedKey(keyValue, thisBlock), MapOperation.getByIndexRange(this.dataBinName, 0, MapReturnType.COUNT));

							if (mapCount == null) {
								// This has potentially split
								blocksInDoubt = true;
							}
							else {
								blockCounts.put(thisBlock, mapCount.getLong(this.dataBinName));
							}
						}
					}

					blockList.clear();
					if (blocksInDoubt) {
						// Re-read the root record to determine what's happened. This should be very rare: (a block TTLd out, or a block split
						// between the time we read the original bitmap and when we read this record)
						Record rootRecord = client.get(null, getCombinedKey(keyValue, 0), BLOCK_MAP_BIN);
						if (rootRecord != null) {
							Map<Long, Long> rootMap = (Map<Long, Long>) rootRecord.getMap(BLOCK_MAP_BIN);
							// If there are any counts we already have which are no longer in the root map, remove them
							Collection<Long> blockSet = rootMap.values();
							for (Iterator<Long> keyIterator = blockCounts.keySet().iterator(); keyIterator.hasNext();) {
								long key = keyIterator.next();
								if (!blockSet.contains(key)) {
									keyIterator.remove();
								}
							}
							blockList = rootMap.values();
						}
					}
				}
				// We have all the counts n the block Counts above.
				int count = 0;
				for (long thisCount : blockCounts.values()) {
					count += thisCount;
				}
	 			return count;

			}
		}
		return 0;
	}

	/**
	 * Get all of the records associated with all of the keys passed in. The returned records will be in the same
	 * order as the input recordKeyValues.
	 */
	@Override
	public <T> List<T>[] getAll(BatchPolicy batchPolicy, String[] recordKeyValues, ObjectMapper<T> mapper) {
		return this.getAll(batchPolicy, recordKeyValues, 0, mapper);
	}

	private class BatchData {
		private long key;
		private int batchIndex;
		private TreeMap<Long, Object> data;
		public BatchData(int batchIndex, long key) {
			super();
			this.key = key;
			this.batchIndex = batchIndex;
		}
		public BatchData(int batchIndex, long key, TreeMap<Long, Object> data) {
			super();
			this.key = key;
			this.batchIndex = batchIndex;
			this.setData(data);
		}
		public int getBatchIndex() {
			return batchIndex;
		}
		public long getKey() {
			return key;
		}
		public TreeMap<Long, Object> getData() {
			return data;
		}
		public void setData(TreeMap<Long, Object> data) {
			this.data = data;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null || (!(obj instanceof BatchData))) {
				return false;
			}
			BatchData data = (BatchData)obj;
			return ((this.key == data.key) && (this.batchIndex == data.batchIndex));
		}
		@Override
		public int hashCode() {
			return ((int)this.key) ^ batchIndex;
		}
	}

	/**
	 * Get all of the records associated with all of the keys passed in. The returned records will be in the same
	 * order as the input recordKeyValues.
	 */
	@Override
	@SuppressWarnings({ "unchecked" })
	public <T> List<T>[] getAll(BatchPolicy batchPolicy, String[] recordKeyValues, long maxRecords, ObjectMapper<T> mapper) {
		final int count = recordKeyValues.length;

		if (maxRecords <= 0) {
			maxRecords = Long.MAX_VALUE;
		}

		Key[] rootKeys = new Key[count];
		for (int i = 0; i < count; i++) {
			rootKeys[i] = getCombinedKey(recordKeyValues[i], 0);
		}
		if (batchPolicy == null) {
			batchPolicy = new BatchPolicy();
		}
		batchPolicy.maxConcurrentThreads = 0;
		Record[] batchResults = client.get(batchPolicy, rootKeys);

		List<BatchData> batchData = new ArrayList<>();
		TreeMap<Long, Long>[] indexArrays = new TreeMap[count];
		for (int i = 0; i < count; i++) {
			Record thisRecord = batchResults[i];
			if (thisRecord != null) {
				TreeMap<Long, Long> blockMap = (TreeMap<Long, Long>) thisRecord.getValue(BLOCK_MAP_BIN);
				if (blockMap != null && !blockMap.isEmpty()) {
					// This block has split, we have the indexes
					indexArrays[i] = blockMap;
					for (long thisKey : blockMap.keySet()) {
						batchData.add(new BatchData(i, blockMap.get(thisKey)));
					}
				}
				else {
					// Just add this to the final record set
					batchData.add(new BatchData(i, 0, (TreeMap<Long, Object>) thisRecord.getMap(dataBinName)));
				}
			}
		}

		while (true) {
			// Guestimate how many records we need.
			long recordEstimate = 0;
			int lastBatchIndex = -1;
			List<Integer> dataToLoad = new ArrayList<>();
			for (int i = 0; i < batchData.size(); i++) {
				BatchData thisBatchData = batchData.get(i);
				if (thisBatchData.getData() != null) {
					recordEstimate += thisBatchData.getData().size();
				}
				else {
					// Assume the records are 75% full unless they're the last one in a block
					if (lastBatchIndex == -1 || lastBatchIndex == thisBatchData.getBatchIndex()) {
						recordEstimate += recordThreshold * 0.75;
					}
					else {
						recordEstimate += recordThreshold * 0.15;
					}
					lastBatchIndex = thisBatchData.getBatchIndex();
					dataToLoad.add(i);
				}
				if (recordEstimate > maxRecords) {
					break;
				}
			}

			if (dataToLoad.size() == 0) {
				break;
			}
			List<Integer> newDataToLoad = new ArrayList<>();
			Key[] keys = new Key[dataToLoad.size()];
			for (int i = 0; i < dataToLoad.size(); i++) {
				BatchData thisData = batchData.get(dataToLoad.get(i));
				keys[i] = getCombinedKey(recordKeyValues[thisData.getBatchIndex()], thisData.getKey());
			}
			Record[] batchGetResults = client.get(batchPolicy, keys);
			for (int i = 0; i < batchGetResults.length; i++) {
				if (batchGetResults[i] != null) {
					batchData.get(dataToLoad.get(i)).data = (TreeMap<Long, Object>) batchGetResults[i].getMap(dataBinName);
				}
				else {
					// TODO: This block has split or been removed. Add it to the list to reload.
					// Also, we should check the number of records, and if not large enough reload next records.
				}
			}
			if (newDataToLoad.size() == 0) {
				break;
			}
			else {
				dataToLoad = newDataToLoad;
			}
		}

		// Should have all the data we need
		final List<T>[] results = new ArrayList[count];
		int recordCount = 0;
		for (BatchData thisBatchData : batchData) {
			TreeMap<Long, Object> data = thisBatchData.getData();
			int index = thisBatchData.batchIndex;
			if (data != null) {
				// This should always be the case.
				for (long key : data.keySet()) {
					if (results[index] == null) {
						results[index] = new ArrayList<T>();
					}
					results[index].add(mapper.map(recordKeyValues[index], key, data.get(key)));
					recordCount++;
					if (recordCount >= maxRecords) {
						return results;
					}
				}
			}
		}
		return results;
	}

	private static class VolatileBoolean {
		public volatile boolean flag;

		public VolatileBoolean(boolean flag) {
			super();
			this.flag = flag;
		}
		public boolean isFlag() {
			return flag;
		}
		public void setFlag(boolean flag) {
			this.flag = flag;
		}
	}

	/**
	 * Get all the records which match the passed operations. The operations are able to do things like "getByValueRange", etc. Each operation
	 * will be applied to all the records which match, and the records returned.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Set<Record> getAll(WritePolicy opPolicy, String keyValue, Operation ... operations) {
		Key rootKey = new Key(namespace, setName, keyValue);
		int numOperations = operations.length;
		Operation[] allOps = new Operation[numOperations + 2];
		for (int i = 0; i < numOperations; i++) {
			allOps[i] = operations[i];
		}
		allOps[numOperations] = Operation.get(BLOCK_MAP_BIN);
		allOps[numOperations+1] = Operation.get(LOCK_BIN);

		Set<Record> records = new HashSet<>();
		Record result = client.operate(opPolicy, rootKey, allOps);

		if (result != null) {
			Map<Long, Long> blockMap = (Map<Long, Long>) result.getMap(BLOCK_MAP_BIN);
			if (blockMap != null && !blockMap.isEmpty()) {
				final VolatileBoolean needsReprocessing = new VolatileBoolean(true);
				Map<Long, Record> retrievedRecords = new HashMap<>();
				while (needsReprocessing.isFlag()) {
					needsReprocessing.setFlag(false);
					Collection<Long> blockList = new ArrayList<Long>(blockMap.values());
					// For each block in the map, load it's data but only if we don't already have it.
					blockList.parallelStream().filter(item -> !retrievedRecords.containsKey(item)).forEach(item -> {
						Key thisKey = getCombinedKey(keyValue, item);
						Record thisResult = client.operate(opPolicy, thisKey, operations);
						if (thisResult != null) {
							retrievedRecords.put(item, thisResult);
						}
						else {
							// There are 2 reasons this might have returned null: either the block has split and been removed whilst we were processing the read
							// or the record has TTLd out. We need to check the root block to check.
							needsReprocessing.setFlag(true);
						}
					});
					if (needsReprocessing.isFlag()) {
						result = client.operate(opPolicy, rootKey, Operation.get(BLOCK_MAP_BIN));
						blockMap = (Map<Long, Long>) result.getMap(BLOCK_MAP_BIN);
					}
				}

				// Now we're guaranteed to have a superset of the blocks in the map, form them into the appropriate result
				for (long key : blockMap.values()) {
					Record thisRecord = retrievedRecords.get(key);
					if (thisRecord != null) {
						// Should always be the case...
						records.add(thisRecord);
					}
				}
			}
			else {
				// This block exists and has not been split, therefore it contains the results.
				records.add(result);
			}
		}
		return records;
	}

	/**
	 * Remove a single key from the Map and return the value which was removed or null if no object was removed.
	 * <p>
	 * Note that deleting records from an underlying map will never cause 2 sub-blocks to merge back together. Hence
	 * removing a large number of records might result in a map with a lot of totally empty sub-blocks.
	 *
	 * @param recordKeyValue
	 * @param mapKey
	 * @param operation
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Object delete(WritePolicy writePolicy, String recordKeyValue, long mapKey) {
		String id = getLockId();
		Operation obtainLock = getObtainLockOperation(id, 0);
		Operation removeFromMap = MapOperation.removeByKey(dataBinName, Value.get(mapKey), MapReturnType.VALUE);
		Operation releaseLock = getReleaseLockOperation(id);
		Operation getBlockMap = Operation.get(BLOCK_MAP_BIN);
		Key key = getCombinedKey(recordKeyValue, 0);

		try {
			Record record = client.operate(writePolicy, key, obtainLock, removeFromMap, getBlockMap, releaseLock);
			if (record == null) {
				return null;
			}
			else {
				return record.getValue(dataBinName);
			}
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() == ResultCode.ELEMENT_EXISTS) {
				while (true) {
					// Since we're removing an element, this can never cause the root block to split.
					Record record = waitForRootBlockToFullyLock(LockType.SUBDIVIDE_BLOCK, key, mapKey, MAX_LOCK_TIME);
					if (record == null) {
						// The lock vanished from under us, maybe it TTLd out, just try again.
						continue;
					}
					else {
						// Check to see which block we should read
						List<Entry<Long, Long>> mapEntries = (List<Entry<Long, Long>>) record.getList(BLOCK_MAP_BIN);
						long block = computeBlockNumber(mapKey, mapEntries);

						try {
							WritePolicy writePolicy2 = writePolicy == null ? new WritePolicy() : new WritePolicy(writePolicy);
							writePolicy2.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
							record = client.operate(writePolicy2, getCombinedKey(recordKeyValue, block),
									obtainLock,
									MapOperation.removeByKey(dataBinName, Value.get(mapKey), MapReturnType.VALUE),
									releaseLock);
							return record.getValue(dataBinName);
						}
						catch (AerospikeException ae1) {
							switch (ae1.getResultCode()) {
								case ResultCode.ELEMENT_EXISTS:
								case ResultCode.KEY_NOT_FOUND_ERROR:
									// Either: The block is locked and in the process of splitting (ELEMENT_EXISTS) or
									// the record has split and been removed (KEY_NOT_FOUND_ERROR). In either case we
									// must re-read the root record and re-attempt the operation.
									try {
										Thread.sleep(2);
									} catch (InterruptedException e) {
									}
									continue;
								default:
									throw ae1;
							}
						}
					}
				}
			}
			else {
				throw ae;
			}
		}
	}
	@Override
	public Object delete(WritePolicy writePolicy, String recordKeyValue, int mapKey) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public Object delete(WritePolicy writePolicy, String recordKeyValue, String mapKey) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public Object delete(WritePolicy writePolicy, String recordKeyValue, byte[] digest) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}


	/**
	 * Execute a UDF on an adaptive map for a select element. Extreme care must be taken when using this function -- the UDF should be allowed to
	 * <ul>
	 * <li>Read the element in the map</li>
	 * <li>Remove the element from the map</li>
	 * <li>Update the value associated with the key</li>
	 * </ul>
	 * The UDF should not insert new elements into the map -- no splitting will result if the UDF increases in items in the map. Also, if the map
	 * increases in size as a result of calling this and overflows the record size this will not cause the map to split.
	 * <p/>
	 * The UDF will be called exactly once if the element is found in the adaptive map with the record containing the block which includes the mapKey.
	 * If the adaptive map does not contain the element, the UDF will not be called at all.
	 * <p/>
	 * Note that no lock is obtained whilst the UDF is being called -- the atomic nature of UDFs in Aerospike will ensure atomicity. However, the
	 * explicit locks used by the adaptive map when splitting ARE observed.
	 * <p/>
	 * When the UDF is called, the first parameter will be the record (as always), the second parameter will be the key in the map which is desired,
	 * the third parameter is the name of the map and the subsequent parameters will be those parameters passed as varargs to this function.
	 * @param writePolicy
	 * @param recordKeyValue
	 * @param mapKey
	 * @param digest
	 * @param packageName
	 * @param functionName
	 * @param args
	 * @return
	 */
	@Override
	public Object executeUdfOnRecord(WritePolicy writePolicy, String recordKeyValue, Object mapKey, byte[] digest, String packageName, String functionName, Value ...args) {
		if (writePolicy == null) {
			writePolicy = new WritePolicy();
		}
		// We must use an inbuilt predExp to see if the block is locked at the root level as this is not an operation
		writePolicy.predExp = new PredExp[] {
			 PredExp.stringVar("lock"),
			 PredExp.stringValue(LOCK_MAP_ENTRY),
			 PredExp.stringEqual(),
			 PredExp.mapBin(LOCK_BIN),
			 PredExp.mapKeyIterateOr("lock"),
			 PredExp.not()
		};
		writePolicy.failOnFilteredOut = true;
		Key key = getCombinedKey(recordKeyValue, 0);

		Value[] values = new Value[args.length+2];
		values[0] = Value.get(mapKey);
		values[1] = Value.get(this.dataBinName);
		for (int i = 0; i < args.length; i++) {
			values[i+2] = args[i];
		}

		try {
			return client.execute(writePolicy, key, packageName, functionName, values);
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
				// This block must have split, find the sub-block and re-execute
				while (true) {
					long numericMapKey;
					if (mapKey instanceof Long || mapKey instanceof Integer || mapKey instanceof Short || mapKey instanceof Byte) {
						numericMapKey = ((Number)mapKey).longValue();
					}
					else {
						throw new java.lang.UnsupportedOperationException("Method not implemented.");
					}

					Record record = waitForRootBlockToFullyLock(LockType.SUBDIVIDE_BLOCK, key, numericMapKey, MAX_LOCK_TIME);
					if (record == null) {
						// The lock vanished from under us, maybe it TTLd out, just try again.
						continue;
					}
					else {
						// Check to see which block we should read
						List<Entry<Long, Long>> mapEntries = (List<Entry<Long, Long>>) record.getList(BLOCK_MAP_BIN);
						long block = computeBlockNumber(numericMapKey, mapEntries);

						try {
							return client.execute(writePolicy, getCombinedKey(recordKeyValue, block), packageName, functionName, values);
						}
						catch (AerospikeException ae1) {
							switch (ae1.getResultCode()) {
								case ResultCode.FILTERED_OUT:
								case ResultCode.KEY_NOT_FOUND_ERROR:
									// Either: The block is locked and in the process of splitting (FILTERED_OUT) or
									// the record has split and been removed (KEY_NOT_FOUND_ERROR). In either case we
									// must re-read the root record and re-attempt the operation.
									try {
										Thread.sleep(2);
									} catch (InterruptedException e) {
									}
									continue;
								default:
									throw ae1;
							}
						}
					}
				}
			}
			else {
				throw ae;
			}
		}
	}

	private String getLockId() {
		return ID + "-" + Thread.currentThread().getId();
	}

	private String getUnlockPlaceholder() {
		return ID + "-" + Long.MAX_VALUE;
	}

	private Operation getObtainLockOperation(String id, long now) {
		if (id == null) {
			id = getLockId();
		}
		List<Object> data = Arrays.asList(new Object[] { id, now + MAX_LOCK_TIME });
		MapPolicy policy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);
		return MapOperation.put(policy, LOCK_BIN, Value.get(LOCK_MAP_ENTRY), Value.get(data));
	}

	private Operation getReleaseLockOperation(String id) {
		if (id == null) {
			id = getLockId();
		}

		List<Object> startData = Arrays.asList(new Object[] { id, Long.MIN_VALUE });
		List<Object> endData = Arrays.asList(new Object[] { id, Long.MAX_VALUE });

		return MapOperation.removeByValueRange(LOCK_BIN, Value.get(startData), Value.get(endData), MapReturnType.RANK);
	}

	private Operation removeUnlockedTagOperation() {
		return MapOperation.removeByKey(LOCK_BIN, Value.get(UNLOCK_MAP_ENTRY), MapReturnType.NONE);
	}

	private Operation getUnlockedTagOperation() {
		List<Object> data = Arrays.asList(new Object[] { getUnlockPlaceholder(), Long.MAX_VALUE });
		MapPolicy policy = new MapPolicy(MapOrder.UNORDERED, 0);
		return MapOperation.put(policy, LOCK_BIN, Value.get(UNLOCK_MAP_ENTRY), Value.get(data));
	}


	private enum LockType {
		SUBDIVIDE_BLOCK (LOCK_BIN);
//		ROOT_BLOCK_SPLIT_IN_PROGRESS (ROOT_BLOCK_LOCK);

		private String binName;
		private LockType(String binName) {
			this.binName = binName;
		}
		public String getBinName() {
			return binName;
		}
	}

	private class LockAcquireException extends AerospikeException {
		public LockAcquireException(Exception e) {
			super(e);
		}
	}

	private class RecordDoesNotExistException extends LockAcquireException {
		public RecordDoesNotExistException(Exception e) {
			super(e);
		}
	}

	private class RootBlockRemoved extends AerospikeException {
		public RootBlockRemoved(String message) {
			super(message);
		}
	}

	private void wait(int delayTime) {
		try {
			Thread.sleep(delayTime);
		} catch (InterruptedException e) {
			throw new AerospikeException(ResultCode.TIMEOUT, false);
		}
	}


	/**
	 * Wait for the root block to complete splitting. The root block, once locked, will never unlock. However, it only
	 * gets locked when it's splitting, so it is expected that the first bit of the block map will be 1. If this is the
	 * case, return. If the block is locked but the first bit of the block map is not 1, wait for it to become 1 or
	 * the lock to expire.
	 *
	 * @param key
	 * @param timeoutInMs
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Record waitForRootBlockToFullyLock(LockType lockType, Key key, long mapKey, long timeoutInMs) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
		while (true) {
			try {
				Record record = client.operate(null, key, getRelevantBlocksOperation(mapKey), Operation.get(lockType.getBinName()));
				if (record == null) {
					return null;
				}
				List<Entry<Long, Object>> blocks = (List<Entry<Long, Object>>)record.getValue(BLOCK_MAP_BIN);
				Map<String, List<Object>> lockData = (Map<String, List<Object>>) record.getMap(lockType.getBinName());
				if (lockData == null || lockData.isEmpty()) {
					// The block is not locked. This is not expected, so return null
					return null;
				}
				else {
					// The lock is held, see if the block has split yet.
					if (blocks != null) {
						// This block has already split
						return record;
					}
					else {
						// See if a timeout has occurred on the lock
						List<Object> lockInfo = lockData.get(LOCK_MAP_ENTRY);
						String lockOwner = (String) lockInfo.get(0);
						long lockExpiry = (long) lockInfo.get(1);
						if (id.equals(lockOwner)) {
							// This thread already owns the lock, done!
							return record;
						}
						else {
							now = new Date().getTime();
							if (now < lockExpiry) {
								if (timeoutInMs > 0 && now >= timeoutEpoch) {
									// It's taken too long to get the lock
									throw new AerospikeException(ResultCode.TIMEOUT, false);
								}
								else {
									// Wait for 1ms and retry
									wait(1);
								}
							}
							else {
								// The lock has expired. Try removing the lock with a gen check
								WritePolicy writePolicy = new WritePolicy();
								writePolicy.generation = record.generation;
								writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
								writePolicy.sendKey = this.sendKey;
								List<Object> data = new ArrayList<>();
								data.add(id);
								data.add(now + MAX_LOCK_TIME);
								try {
									return client.operate(writePolicy, key, MapOperation.clear(lockType.getBinName()), Operation.get(BLOCK_MAP_BIN));
								}
								catch (AerospikeException ae2) {
									if (ae2.getResultCode() == ResultCode.GENERATION_ERROR) {
										// A different thread obtained the lock before we were able to do so, retry
									}
									else {
										throw ae2;
									}
								}
							}
						}

					}
				}
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
					// We tried to obtain the lock which was not set, but a write occurred updating the block. Wait and try
					wait(1);
				}
				else {
					throw ae;
				}
			}
		}
	}
	/**
	 * Wait for the root block to complete splitting. The root block, once locked, will never unlock. However, it only
	 * gets locked when it's splitting, so it is expected that map of minInt -> blockNum will not be null. If this is the
	 * case, return. If the block is locked but the block map is empty, wait for it to become populated or
	 * the lock to expire. If the lock expires, assume the writing process has died, acquire the lock ourselves and perform the split.
	 *
	 * @param key
	 * @param timeoutInMs
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Record waitForRootBlockLock(LockType lockType, Key key, long mapKey, long timeoutInMs) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
		while (true) {
			try {
				Record record = client.operate(null, key,
						getRelevantBlocksOperation(mapKey),
						Operation.get(lockType.getBinName()));

				if (record == null) {
					return null;
				}
				List<Entry<Long, Long>> mapKeys = (List<Entry<Long, Long>>) record.getList(BLOCK_MAP_BIN);
				Map<String, List<Object>> lockData = (Map<String, List<Object>>) record.getMap(lockType.getBinName());
				if (lockData == null || lockData.isEmpty()) {
					// The block is not locked, acquire
					Operation acquireLock = getObtainLockOperation(id, now);
					WritePolicy wp = new WritePolicy();
					wp.generation = record.generation;
					wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
					client.operate(wp, key, acquireLock);
					return record;
				}
				else {
					// The lock is held, see if the block has split yet.
					if (mapKeys != null) {
						// This block has already split
						return record;
					}
					else {
						// See if a timeout has occurred on the lock
						List<Object> lockInfo = lockData.get(LOCK_MAP_ENTRY);
						String lockOwner = (String) lockInfo.get(0);
						long lockExpiry = (long) lockInfo.get(1);
						if (id.equals(lockOwner)) {
							// This thread already owns the lock, done!
							return record;
						}
						else {
							now = new Date().getTime();
							if (now < lockExpiry) {
								if (timeoutInMs > 0 && now >= timeoutEpoch) {
									// It's taken too long to get the lock
									throw new AerospikeException(ResultCode.TIMEOUT, false);
								}
								else {
									// Wait for 1ms and retry
									wait(1);
								}
							}
							else {
								// The lock has expired. Try to force reacquiring the lock with a gen check
								WritePolicy writePolicy = new WritePolicy();
								writePolicy.generation = record.generation;
								writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
								writePolicy.sendKey = this.sendKey;

								List<Object> data = new ArrayList<>();
								data.add(id);
								data.add(now + MAX_LOCK_TIME);
								MapPolicy forcePolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
								Operation dataOperation = Operation.get(dataBinName);
								try {
									record = client.operate(writePolicy, key, MapOperation.put(forcePolicy, lockType.getBinName(), Value.get(LOCK_MAP_ENTRY), Value.get(data)), dataOperation);
									return record;
								}
								catch (AerospikeException ae2) {
									if (ae2.getResultCode() == ResultCode.GENERATION_ERROR) {
										// A different thread obtained the lock before we were able to do so, retry
									}
									else {
										throw ae2;
									}
								}
							}
						}

					}
				}
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
					// We tried to obtain the lock which was not set, but a write occurred updating the block. Wait and try
					wait(1);
				}
				else {
					throw ae;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Record waitForLockRelease(LockType lockType, Key key, long timeoutInMs) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		int currentDelay = 1;
		while (true) {
			try {
				Record record = client.get(null, key, lockType.getBinName());
				if (record == null) {
					return null;
				}
				Map<String, List<Object>> lockData = (Map<String, List<Object>>) record.getMap(lockType.getBinName());
				if (lockData == null || lockData.isEmpty()) {
					// The block is not locked, done!
					return record;
				}
				else {
					// The lock is held, see if a timeout has occurred on the lock
					List<Object> lockInfo = lockData.get(LOCK_MAP_ENTRY);
					long lockExpiry = (long) lockInfo.get(1);
					now = new Date().getTime();
					if (now < lockExpiry) {
						if (timeoutInMs > 0 && now >= timeoutEpoch) {
							// It's taken too long to get the lock
							throw new AerospikeException(ResultCode.TIMEOUT, false);
						}
						else {
							// Wait for 1ms and retry
							wait(currentDelay);
							if (currentDelay < 8) {
								currentDelay *= 2;
							}
						}
					}
					else {
						// The lock has expired. Try to force removing the lock with a gen check
						WritePolicy writePolicy = new WritePolicy();
						writePolicy.generation = record.generation;
						writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
						writePolicy.sendKey = this.sendKey;

						Operation dataOperation = Operation.get(dataBinName);
						Operation clearMapOperation = Operation.put(Bin.asNull(lockType.binName));
						try {
							record = client.operate(writePolicy, key, clearMapOperation, dataOperation);
							return record;
						}
						catch (AerospikeException ae2) {
							if (ae2.getResultCode() == ResultCode.GENERATION_ERROR) {
								// A different thread obtained the lock before we were able to do so, retry
							}
							else {
								throw ae2;
							}
						}
					}
				}
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
					// We tried to obtain the lock which was not set, but a write occurred updating the block. Wait and try
					wait(1);
				}
				else {
					throw ae;
				}
			}
		}
	}

	/**
	 * Acquire a lock used for splitting an existing block into 2. Since this is an existing block, the lock must exist
	 * unless the entire block has TTLd out, or just been removed after a block has split.
	 * @param key
	 * @param timeoutInMs
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Record acquireLock(LockType lockType, Key key, long mapKey, long timeoutInMs, boolean getData) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
		Operation dataOperation = getData ? Operation.get(dataBinName) : null;
		while (true) {
			try {
				List<Object> data = new ArrayList<>();
				data.add(id);
				data.add(now + MAX_LOCK_TIME);
				WritePolicy wp = new WritePolicy();
				wp.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
				wp.sendKey = this.sendKey;
				if (dataOperation != null) {
					Record record = client.operate(wp, key, getObtainLockOperation(id, now), removeUnlockedTagOperation(), getRelevantBlocksOperation(mapKey), dataOperation);
					return record;
				}
				else {
					client.operate(wp, key, getObtainLockOperation(id, now), removeUnlockedTagOperation());
					return null;
				}
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
					throw new RecordDoesNotExistException(ae);
				}
				else if (ae.getResultCode() == ResultCode.ELEMENT_EXISTS) {
					// the lock is already owned
					Record record;
					if (getData) {
						record = client.operate(null, key, Operation.get(lockType.getBinName()), getRelevantBlocksOperation(mapKey), Operation.get(dataBinName));
					}
					else {
						record = client.operate(null, key, Operation.get(lockType.getBinName()), getRelevantBlocksOperation(mapKey));
					}
					if (record == null) {
						throw new RecordDoesNotExistException(ae);
					}
					else {
						Map<String, List<Object>> lockData = (Map<String, List<Object>>) record.getMap(lockType.getBinName());
						if (lockData == null || lockData.isEmpty()) {
							// The lock seems to be released, continue to re-acquire
							continue;
						}
						else {
							List<Object> lockInfo = lockData.get(LOCK_MAP_ENTRY);
							String lockOwner = (String) lockInfo.get(0);
							long lockExpiry = (long) lockInfo.get(1);
							if (id.equals(lockOwner)) {
								// This thread already owns the lock, done!
								return record;
							}
							else {
								now = new Date().getTime();
								if (now < lockExpiry) {
									if (timeoutInMs > 0 && now >= timeoutEpoch) {
										// It's taken too long to get the lock
										throw new AerospikeException(ResultCode.TIMEOUT, false);
									}
									else {
										wait(1);
									}
								}
								else {
									// The lock has expired. Try to force reacquiring the lock with a gen check
									WritePolicy writePolicy = new WritePolicy();
									writePolicy.generation = record.generation;
									writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
									writePolicy.sendKey = this.sendKey;

									List<Object> data = new ArrayList<>();
									data.add(id);
									data.add(now + MAX_LOCK_TIME);
									MapPolicy forcePolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
									try {
										if (dataOperation != null) {
											record = client.operate(writePolicy, key, MapOperation.put(forcePolicy, lockType.getBinName(), Value.get(LOCK_MAP_ENTRY), Value.get(data)), Operation.get(BLOCK_MAP_BIN), dataOperation);
											return record;
										}
										else {
											client.operate(writePolicy, key, MapOperation.put(forcePolicy, lockType.getBinName(), Value.get(LOCK_MAP_ENTRY), Value.get(data)), Operation.get(BLOCK_MAP_BIN));
											return null;
										}
									}
									catch (AerospikeException ae2) {
										if (ae2.getResultCode() == ResultCode.GENERATION_ERROR) {
											// A different thread obtained the lock before we were able to do so, retry
										}
										else {
											throw ae;
										}
									}
								}
							}
						}
					}
				}
				else {
					throw ae;
				}
			}
		}

	}

	/**
	 * Release the lock on this record, but only if we own it.
	 * <p>
	 * Note that at the moment this is not needed because we only lock a block when it's splitting. The root block is never unlocked
	 * and the child blocks are removed instead of being unlocked (and the remove obviously removes the lock too)
	 *
	 * @param recordKey - The key of the record on which the lock is to be released
	 * @return -  true if this thread owned the lock and the lock was successfully released, false if this thread did not own the lock.
	 */
	@SuppressWarnings("unused")
	private boolean releaseLock(LockType lockType, String recordKey, int blockNum) {
		return releaseLock(lockType, getCombinedKey(recordKey, blockNum));
	}

	private boolean releaseLock(LockType lockType, Key key) {
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.maxRetries = 5;
		writePolicy.sleepBetweenRetries = 100;
		Record record = client.operate(writePolicy, key, getReleaseLockOperation(null));
		if (record != null) {
			return record.getList(lockType.getBinName()) != null && record.getList(lockType.getBinName()).size() == 1;
		}
		return false;
	}

	private boolean areBlockListsEqual(List<Entry<Long, Long>> originalList, List<Entry<Long, Long>> newList) {
		if (originalList == null && newList == null) {
			return true;
		}
		else if ((originalList == null && newList != null) || (originalList != null && newList == null)) {
			return false;
		}
		else if (originalList.size() != newList.size()) {
			return false;
		}
		else {
			for (int i = 0; i < originalList.size(); i++) {
				Entry<Long, Long> orig = originalList.get(i);
				Entry<Long, Long> neww = newList.get(i);
				if ((orig.getKey() != neww.getKey()) || (orig.getValue() != neww.getValue())) {
					return false;
				}
			}
		}
		return true;
	}
	/**
	 * A block has overflowed, it must be subdivided into 2 smaller blocks. The general principal is:<br>
	 * <ol>
	 * <li>Obtain the block lock on the record and read all of its data. This prevents other data being written whilst the lock is held</li>
	 * <li>Split the data on the block into 2 set of record</li>
	 * <li>Write the new records</li>
	 * <li>Obtain the block map lock, update the block map, unlock the block map lock</li>
	 * <li>Remove the split block (and hence remove the lock which exists on this record too)</li>
	 * </ol>
	 * <p>
	 * These operations are designed in this order so that if the process dies in the middle of them, the next attempt to re-perform them
	 * will succeed without having to rollback any of these steps.
	 * @param Write
	 * @param recordKey
	 * @param blockNum
	 * @param mapKey
	 * @param dataInsertOp
	 * @return true if the operation was successful, false if the operation failed but it is a recoverable error where retrying may succeed.
	 */
	private boolean splitBlockAndInsert(WritePolicy writePolicy, String recordKey, long blockNum, long mapKey, Object dataValue, List<Entry<Long, Long>> blockMap, int blockMapGeneration) {
		return splitBlockAndInsert(writePolicy, recordKey, blockNum, mapKey, dataValue, blockMap, blockMapGeneration, null, 0);
	}

	/**
	 * A block has overflowed, it must be subdivided into 2 smaller blocks. The general principal is:<br>
	 * <ol>
	 * <li>Obtain the block lock on the record and read all of its data. This prevents other data being written whilst the lock is held</li>
	 * <li>Split the data on the block into 2 set of record</li>
	 * <li>Write the new records</li>
	 * <li>Obtain the block map lock, update the block map, unlock the block map lock</li>
	 * <li>Remove the split block (and hence remove the lock which exists on this record too)</li>
	 * </ol>
	 * <p>
	 * These operations are designed in this order so that if the process dies in the middle of them, the next attempt to re-perform them
	 * will succeed without having to rollback any of these steps.
	 * @param writePolicy - The write policy to use, useful for retries, etc. Some parts of this policy may be overwritten by this method.
	 * @param recordKey -  The base key of the record. This is not the block to split. For example, if records are being inserted into "baseKey1" object
	 * 						and this is block baseKey1:12, this string should be "baseKey1"
	 * @param blockNum - The block number to split
	 * @param mapKey - The key to insert into the map.
	 * @param dataValue - The value to insert into the map.
	 * @param blockMap - The current block map. Can be <code>null</code>, which will force a database read.
	 * @param blockMapGeneration - The generation of the block map. If this value is < 0 the blockMap parameter will be ignored and the value re-read from the database
	 * @param data - The data of the record. If this is <code>null</code> it will be read from the database
	 * @param dataTTL - the TTL of this subblock. If the <code>data</code> parameter above is <code>null</code>, this will be read from the record along with the data
	 * 					and the passed value ignored.
	 * @return <code>true</code> if the operation was successful, <code>false</code> if the operation failed but could conceivably succeed on a retry, eg a race condition.
	 */
	@SuppressWarnings("unchecked")
	private boolean splitBlockAndInsert(WritePolicy writePolicy, String recordKey, long blockNum, long mapKey, Object dataValue, List<Entry<Long, Long>> blockMap, int blockMapGeneration, TreeMap<Long, Object> data, int dataTTL) {
		// We must have a valid block map, it may or may not have been provided. If the root block has not split, we will validly have a record with a positive generation and
		// a null block map
		if (blockMapGeneration < 0) {
			Record record = client.operate(null, getCombinedKey(recordKey, 0), getRelevantBlocksOperation(mapKey));
			if (record != null) {
				blockMap = (List<Entry<Long, Long>>)record.getValue(BLOCK_MAP_BIN);
				blockMapGeneration = record.generation;
			}
			else {
				// The root block has gone away! This is unexpected, since to get here we must have been able to read it earlier in this operation.
				throw new RootBlockRemoved("Root block removed whilst splitting block " + blockNum  + " on record " + recordKey);
			}
		}

		boolean isRootBlock = blockNum == 0;

		// We want to get the lock and retrieve all the data in one hit
		Key key = getCombinedKey(recordKey, blockNum);
		if (data == null) {
			Record currentRecord;
			try {
				currentRecord = acquireLock(LockType.SUBDIVIDE_BLOCK, key, mapKey, MAX_LOCK_TIME, true);
			}
			catch (RecordDoesNotExistException rdnee) {
				// The lock could not be obtained because the underlying record has been removed. THis could be because another
				// thread already split it. Retry the insert again
				return false;
			}
			// It is possible that the block has been split by another thread since we determined we needed to split it. Only applies to root block
			List<Entry<Long, Long>> newBlockMap = (List<Entry<Long, Long>>)currentRecord.getValue(BLOCK_MAP_BIN);
			if (isRootBlock && !areBlockListsEqual(blockMap, newBlockMap)) {
				return false;
			}

			data = (TreeMap<Long, Object>)currentRecord.getMap(dataBinName);
			dataTTL = currentRecord.getTimeToLive();
			if (blockNum == 0) {
				// We're locking the root block which does a write, hence we must update the generation counter
				blockMapGeneration = currentRecord.generation;
			}
		}

		// Add in the current record under consideration
		data.put(mapKey, dataValue);

		// Create the 2 halves of the map and populate them.
		int dataSize = data.size();
		int splitIndex = dataSize / 2;

		// If we're the last block then we want to split it unevenly.
		if (isLastBlock(mapKey, blockMap) && recordThreshold  >= 5 && dataSize > recordThreshold) {
			splitIndex = recordThreshold - 2;
		}

		List<List<Entry<Long, Object>>> dataHalves = new ArrayList<>();
		dataHalves.add(new ArrayList<Entry<Long,Object>>(dataSize));
		dataHalves.add(new ArrayList<Entry<Long,Object>>(dataSize));

		int counter = 0;
		NavigableSet<Long> keySet = data.navigableKeySet();
		for (Iterator<Long> keyIterator = keySet.iterator(); keyIterator.hasNext(); counter++) {
			long thisKey = keyIterator.next();
			Entry<Long, Object> entry = new AbstractMap.SimpleEntry<Long, Object>(thisKey, data.get(thisKey));
			dataHalves.get(counter <= splitIndex ? 0 : 1).add(entry);
		}

		// Now write the new records. In theory they should be CREATE_ONLY, but if we use this flag and another thread got part of the
		// way through splitting this block and died, the records might exist and it would cause this process to fail.
		// Note that we must preserve the TTL of the current time.
		if (writePolicy == null) {
			writePolicy = new WritePolicy();
		}
		writePolicy.expiration = dataTTL;
		writePolicy.sendKey = this.sendKey;
		writePolicy.recordExistsAction = RecordExistsAction.UPDATE;

		long firstHalfKey = generateUniqueNumber();
		long secondHalfKey = generateUniqueNumber();

		client.operate(writePolicy, getCombinedKey(recordKey, firstHalfKey),
				Operation.put(new Bin(dataBinName, dataHalves.get(0), MapOrder.KEY_ORDERED)), getUnlockedTagOperation());
		client.operate(writePolicy, getCombinedKey(recordKey, secondHalfKey),
				Operation.put(new Bin(dataBinName, dataHalves.get(1), MapOrder.KEY_ORDERED)), getUnlockedTagOperation());

		// The sub-records now exist, update the map and delete this record
		long firstAddress = dataHalves.get(0).get(0).getKey();
		long secondAddress = dataHalves.get(1).get(0).getKey();
		if (isRootBlock) {
			// this is the root block. The entry into the map for the first part should be Long.MIN_VALUE
			firstAddress = Long.MIN_VALUE;
		}

		updateRootBlockMap(recordKey, isRootBlock, firstAddress, firstHalfKey, secondAddress, secondHalfKey);
		if (!isRootBlock) {
			// This does not need to be done durably. If it comes back it will just TTL later
			if (forceDurableDeletes) {
				writePolicy.durableDelete = true;
			}
			client.delete(writePolicy, key);
		}
		return true;
	}


	private void updateRootBlockMap(String recordKey, boolean isRootBlock, long firstIndex, long firstBlock, long secondIndex, long secondBlock) {
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.sendKey = this.sendKey;

		Key key = getCombinedKey(recordKey, 0);

		MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, 0);
		if (isRootBlock) {
			// The root block has split, remove the data from the root block. Do NOT release the lock as this is a flag to imply we've split.
			// Also add in a new bin to identifiy this as the root of an adaptive map.
			client.operate(writePolicy, key,
					MapOperation.put(mapPolicy, BLOCK_MAP_BIN, Value.get(firstIndex), Value.get(firstBlock)),
					MapOperation.put(mapPolicy, BLOCK_MAP_BIN, Value.get(secondIndex), Value.get(secondBlock)),
					Operation.put(new Bin(ROOT_INDICATOR, 2)),
					Operation.put(Bin.asNull(dataBinName)));
		}
		else {
			client.operate(writePolicy, key,
					MapOperation.put(mapPolicy, BLOCK_MAP_BIN, Value.get(firstIndex), Value.get(firstBlock)),
					MapOperation.put(mapPolicy, BLOCK_MAP_BIN, Value.get(secondIndex), Value.get(secondBlock)));
		}
	}

	/**
	 * This method is called when the root block has already split and we need to insert the record into a block which is not block zero.
	 * <p/>
	 *
	 * @param recordKey - The primary key of the root record.
	 * @param blockNum - The sub-block number to insert this into. Should be > 0
	 * @param mapKey - The key to insert this into the map with
	 * @param mapKeyDigest - The digest of the key. Can be null in which case the digest will be computed if needed
	 * @param value - The value to store in the map.
	 * @return - true if the operation succeeded, false if the operation needs to re-read the root block and retry.
	 */
	private boolean putToSubBlock(WritePolicy writePolicy, String recordKey, long blockNum, long mapKey, Value value, List<Entry<Long, Long>> blockMap, int blockMapGeneration) {
		if (writePolicy == null) {
			writePolicy = new WritePolicy();
		}
		writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

		// We want to insert into the map iff the record is not locked. Hence, we will write the lock with CREATE_ONLY, insert
		// the record, then delete the lock. If the operation succeeds, we know the lock was not held during the write
		// Note that as we never actually hold the lock, we don't care about the lock time value
		String id = getLockId();
		Operation obtainLock = getObtainLockOperation(id, 0);
		Operation addToMap = MapOperation.put(mapPolicy, dataBinName, Value.get(mapKey), value);
		Operation releaseLock = getReleaseLockOperation(id);

		Key key = getCombinedKey(recordKey, blockNum);

		try {
			Record result = client.operate(writePolicy, key, obtainLock, addToMap, releaseLock);
			int recordsInBlock = result.getInt(dataBinName);
			if (recordsInBlock > this.recordThreshold) {
				return splitBlockAndInsert(writePolicy, recordKey, blockNum, mapKey, value.getObject(), blockMap, blockMapGeneration);
			}
			return true;
		}
		catch (AerospikeException ae) {
			switch (ae.getResultCode()) {
				case ResultCode.ELEMENT_EXISTS:
					// The block is locked. Wait for the lock to expire or be removed
					waitForLockRelease(LockType.SUBDIVIDE_BLOCK, key, MAX_LOCK_TIME);
					return false;

				case ResultCode.KEY_NOT_FOUND_ERROR:
					// The record has split and been removed (KEY_NOT_FOUND_ERROR).
					// Re-read the root record and re-attempt the operation.
					return false;

				case ResultCode.RECORD_TOO_BIG:
					// This record is too big, try to resplit it.
					return splitBlockAndInsert(writePolicy, recordKey, blockNum, mapKey, value.getObject(), blockMap, blockMapGeneration);

				default:
					throw ae;
			}
		}
	}

	/**
	 * Put a record to the adaptive map
	 * @param recordKey - the key to the record to use
	 * @param mapKey = the key to insert into the map
	 * @param value - the value associate with the key.
	 */
	@SuppressWarnings("unchecked")
	public void put(WritePolicy writePolicy, String recordKey, long mapKey, Value value) {

		// We insert into the root record iff the Lock is not held. If the lock is held it means that this block
		// has split and we must read further.
		if (writePolicy == null) {
			writePolicy = new WritePolicy();
		}
		writePolicy.recordExistsAction = RecordExistsAction.UPDATE;

		// We want to insert into the map iff the record is not locked. Hence, we will write the lock with CREATE_ONLY, insert
		// the record, then delete the lock. If the operation succeeds, we know the lock was not held during the write
		// Note that as we never actually hold the lock, we don't care about the lock time value

		// NB: Under Aerospike 4.7 or later we could use a predicate expression ontop of the operation to simplify this. (and remove the locking operations)
		String id = getLockId();
		Operation obtainLock = getObtainLockOperation(id, 0);
		Operation addToMap = MapOperation.put(mapPolicy, dataBinName, Value.get(mapKey), value);
		Operation releaseLock = getReleaseLockOperation(id);
		Operation getBlockMap = getRelevantBlocksOperation(mapKey);
//		Operation getBlockMap = Operation.get(BLOCK_MAP_BIN);

		Key key = getCombinedKey(recordKey, 0);

		try {
			while (true) {
				Record result = client.operate(writePolicy, key, obtainLock, addToMap, getBlockMap, releaseLock);
				int recordsInBlock = result.getInt(dataBinName);
				if (recordsInBlock > this.recordThreshold) {
					if (splitBlockAndInsert(writePolicy, recordKey, 0, mapKey, value.getObject(), (List<Entry<Long, Long>>)result.getValue(BLOCK_MAP_BIN), result.generation)) {
						// If this returns true, the operation was successful. If it returns false, the block split before we could obtain the lock, so retry
						break;
					}
				}
				else {
					break;
				}
			}
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() == ResultCode.ELEMENT_EXISTS) {
				while (true) {
					// The lock already existing at the root level means that this block has split or is in the process of splitting, this lock will
					// never get cleared once set. Thus, load the Block map and compute where to store the data. If the bit 0 of the block map is not
					// set, it means that the root block is still splitting, must wait
					Record record = waitForRootBlockLock(LockType.SUBDIVIDE_BLOCK, key, mapKey, MAX_LOCK_TIME);
					if (record == null) {
						// The block vanished from under us, maybe it TTLd out, just try again. Note this is a VERY unusual case, so recursion here should
						// be ok and not cause any stack overflow
						put(writePolicy, recordKey, mapKey, value);
						break;
					}
					else {
						// Either the block has split or we must split it.
						List<Entry<Long, Long>> blockMap = (List<Entry<Long, Long>>) record.getList(BLOCK_MAP_BIN);
						if (blockMap != null) {
							// The block has already split, we need to update the appropriate sub-block

							long blockToAddTo = computeBlockNumber(mapKey, blockMap);
							if (putToSubBlock(writePolicy, recordKey, blockToAddTo, mapKey, value, blockMap, record.generation)) {
								break;
							}
						}
						else {
							// We own the lock, but must split this block
							if (splitBlockAndInsert(writePolicy, recordKey, 0L, mapKey, value.getObject(), blockMap, record.generation, (TreeMap<Long,Object>)record.getMap(dataBinName), record.getTimeToLive())) {
								break;
							}
						}
					}
				}
			}
			else if (ae.getResultCode() == ResultCode.RECORD_TOO_BIG) {
				splitBlockAndInsert(writePolicy, recordKey, 0, mapKey, value.getObject(), null, -1);
			}
			else {
				throw ae;
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void deleteAll(WritePolicy writePolicy, String recordKeyValue) {
		Key key = getCombinedKey(recordKeyValue, 0);
		Record record = client.get(null, key);
		if (record != null) {

			if (forceDurableDeletes) {
				writePolicy.durableDelete = true;
			}
			client.delete(writePolicy, key);
			// Now delete all the sub-blocks.
			Map<Long, Long> blockMap = (Map<Long, Long>) record.getValue(BLOCK_MAP_BIN);

			writePolicy.maxRetries = 5;
			writePolicy.totalTimeout = 5000;
			writePolicy.sleepBetweenRetries = 250;
			for (long block : blockMap.values()) {
				if (block > 0) {
					client.delete(writePolicy, getCombinedKey(recordKeyValue, block));
				}
			}

		}
	}

	public static void main(String[] args) {
		boolean seed = true;
		IAerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		if (seed) {
			client.truncate(null, "test", "testAdaptive", null);
		}
		AdaptiveMapUserSuppliedKey map = new AdaptiveMapUserSuppliedKey(client, "test", "testAdaptive", "map", new MapPolicy(MapOrder.KEY_ORDERED, 0), 100);

		for (long i = 0; i < 200; i+=10) {
			Value value = Value.get("Key-" + i);
			map.put(null, "testKey", i, value);
		}

		map.put(null,  "testKey", 161, Value.get("K-161"));
		map.put(null,  "testKey", 167, Value.get("K-167"));
		map.put(null,  "testKey", 162, Value.get("K-162"));
		map.put(null,  "testKey", 16, Value.get("K-16"));
		map.put(null,  "testKey", 168, Value.get("K-168"));
		map.put(null,  "testKey", 169, Value.get("K-169"));
		map.put(null,  "testKey", 163, Value.get("K-163"));
		map.put(null,  "testKey", 164, Value.get("K-164"));
		map.put(null,  "testKey", 165, Value.get("K-165"));

		System.out.println("----------------");
		List<String> data = map.getAll(null, "testKey", (recordKey, key, recordData) -> {return key + ": " + recordData.toString();} );
		for (String key : data) {
			System.out.println(key);
		}

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.sendKey = true;
		String[] keys = new String[30];
		for (int i = 0; i < 30; i++) {
			keys[i] = "test-" + i;
			if (seed) {
//				for (int j = 0; j < 2+(i*30); j++) {
				for (int j = 0; j < 200000; j++) {
					Value value =Value.get("Data-"+i+"-"+j+"  123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890");
					map.put(writePolicy, keys[i], 100*i + j, value);
				}
			}
		}
		for (int c = 0; c < 100; c++) {
			int desiredCount = 10000;
			long now = System.nanoTime();
			List<String>[] resultData = map.getAll(null, keys, desiredCount, (recordKey, key, recordData) -> {return key +": " + recordData.toString(); } );
			long time = System.nanoTime() - now;
			System.out.printf("getting %d records took %,.3fms\n", desiredCount, time / 1000000.0);
			if (c == 99) {
				int count = 0;
				for (int i = 0; i < 30; i++) {
					System.out.println(i + " -" + resultData[i]);
					if (resultData[i] != null) {
						count += resultData[i].size();
					}
				}
				System.out.println(count);
			}
		}
		for (int i = 0; i < 30; i++) {
			System.out.printf("%d - %d\n", i, map.countAll(null, "test-"+i));
		}
		client.close();

	}

	@Override
	public Hash getHashFunction() {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}
	@Override
	public void setHashFuction(Hash hash) {
		throw new java.lang.UnsupportedOperationException("Method not implemented.");
	}

}
