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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

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
import com.aerospike.client.util.Crypto;

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
public class AdaptiveMap implements IAdaptiveMap {
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
	/**
	 * The lock to be used on the root node when the block map is changing. This record will also have a LOCK_BIN
	 * for when it's adjusting 
	 */
	/**
	 * The maximum time any lock should be held for during the splitting of a record
	 */
	private static final long MAX_LOCK_TIME = 100;
	/**
	 * A moderately unique ID 
	 */
	private static final String ID = UUID.randomUUID().toString(); // This needs to be unique only for this session, used only for locking

	private final boolean sendKey = true;
	private final IAerospikeClient client;
	private final String namespace;
	private final String setName;
	private final BitwiseData bitwiseOperations = new BitwiseData();

	private final MapPolicy mapPolicy;
	private final boolean useDigestForMapKey;
	private final int recordThreshold;
	private final boolean forceDurableDeletes;

	private static Value objectToValue(Object obj) {
		if (obj instanceof Long) {
			return Value.get(((Long)obj).longValue());
		}
		else if (obj instanceof Integer) {
			return Value.get(((Integer)obj).intValue());
		}
		else if (obj instanceof String) {
			return Value.get(((String)obj));
		}
		else {
			return Value.get(obj);
		}
	}
	private Hash hashFunction = (key) -> {
		return Crypto.computeDigest("", objectToValue(key));
	};

	/**
	 * Create a new adaptive map
	 * 
	 * @param client - the AerospikeClient used to manipulate this map
	 * @param namespace - The namespace for the map
	 * @param setName - The set name to use for the map
	 * @param mapBin - The bin name to store the bin in. Note that this class reserves the right to create other bins on the record
	 * to store things like lock information
	 * @param mapPolicy - The mapPolicy to use when inserting an element into the map
	 * @param useDigestForMapKey - Whether to use the digest of the map key for the map key or not. If the natural key is abstract (eg a transaction id)
	 * and is not needed in it's own right, it can be more efficient to store the digest of this natural key in the abstract map rather than actual key. Obviously
	 * if the map key has meaning (eg a timestamp) then this cannot be stored as a digest
	 * <p>
	 * The advantage of storing the digest as a map key rather than the natural key is that it is more efficient when a block (sub-part of a map) splits.
	 * Consider a case where the block contains 1,000 elements and has to split: 
	 * <p>
	 * The block splits into 2 blocks, both of which contain around 500 elements. In order to get a deterministic yet moderately equal split, a particular 
	 * bit in the digest is used. If this bit is 0 the record belongs to one block, if it's 1 then the record belongs to the other block. However, in order
	 * to make this determination, the digest of each map entry is needed. If the digest is stored as the key then this is trivial to determine. However,
	 * if the natural key is stored, the digest must be computed for each record in the block. 
	 * @param recordThreshold - How many records can be stored in a block before the block splits. For example, if this number is set to 
	 * 1,000, the 1,001st record would cause the block to split on inserting. It is possible that the block will split before this however --
	 * if the insertion of a map entry results in a RECORD_TOO_BIG exception, the block will split automatically if there is > 1 entry.
	 * @param forceDurableDeletes - If true, all deletes will be done durably irrespective of the durableDelete flag passed in the write policy. If false, the writePolicy will be honored.
	 * For strong consistency namespaces with strong-consistency-allow-expunges set to false, this flag should be true.
	 */
	public AdaptiveMap(final IAerospikeClient client, final String namespace, final String setName, final String mapBin, final MapPolicy mapPolicy, final boolean useDigestForMapKey, int recordThreshold, boolean forceDurableDeletes) {
		this.client = client;
		this.setName = setName;
		this.namespace = namespace;
		this.mapPolicy = mapPolicy == null ? new MapPolicy(MapOrder.KEY_ORDERED, 0) : mapPolicy;
		this.useDigestForMapKey = useDigestForMapKey;
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
	 * @param useDigestForMapKey - Whether to use the digest of the map key for the map key or not. If the natural key is abstract (eg a transaction id)
	 * and is not needed in it's own right, it can be more efficient to store the digest of this natural key in the abstract map rather than actual key. Obviously
	 * if the map key has meaning (eg a timestamp) then this cannot be stored as a digest
	 * <p>
	 * The advantage of storing the digest as a map key rather than the natural key is that it is more efficient when a block (sub-part of a map) splits.
	 * Consider a case where the block contains 1,000 elements and has to split: 
	 * <p>
	 * The block splits into 2 blocks, both of which contain around 500 elements. In order to get a deterministic yet moderately equal split, a particular 
	 * bit in the digest is used. If this bit is 0 the record belongs to one block, if it's 1 then the record belongs to the other block. However, in order
	 * to make this determination, the digest of each map entry is needed. If the digest is stored as the key then this is trivial to determine. However,
	 * if the natural key is stored, the digest must be computed for each record in the block. 
	 * @param recordThreshold - How many records can be stored in a block before the block splits. For example, if this number is set to 
	 * 1,000, the 1,001st record would cause the block to split on inserting. It is possible that the block will split before this however --
	 * if the insertion of a map entry results in a RECORD_TOO_BIG exception, the block will split automatically if there is > 1 entry.
	 */
	public AdaptiveMap(final IAerospikeClient client, final String namespace, final String setName, final String mapBin, final MapPolicy mapPolicy, final boolean useDigestForMapKey, int recordThreshold) {
		// We need to determine if durable deletes must be done irrespective of the write policy, so look at the namespace info for this.
		this(client, namespace, setName, mapBin, mapPolicy, useDigestForMapKey, recordThreshold, forceDurableDeletes(client, namespace));
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
	 * Set the hash function this class will use. If not set, will default to RIPEMD160
	 * @param hash
	 */
	public void setHashFuction(Hash hash) {
		if (hash == null) {
			throw new NullPointerException("hashFunction cannot be set to null");
		}
		this.hashFunction = hash;
	}
	
	public Hash getHashFunction() {
		return this.hashFunction;
	}
	
	public void computeBlocks(byte[] splitter, int fromPosition, List<Integer> results) {
		if (!bitwiseOperations.getBit(splitter, fromPosition)) {
			// This block hasn't split, so just return this one.
			results.add(fromPosition);
		}
		else {
			// This block has split, do not return it, just it's children
			// As the bits are 0 based, the 2 children of block N are 2N+1 and 2N+2
			computeBlocks(splitter, 2*fromPosition+1, results);
			computeBlocks(splitter, 2*fromPosition+2, results);
		}
	}

	/**
	 * This method basically computes Math.log2(node+1). So:
	 *		0 -> 0
	 *		1,2 -> 1
	 *		3,4,5,6 -> 2
	 *		7,8,9,10,11,12,13,14 -> 3
	 *		etc
	 * @param node
	 * @return
	 */
	protected int depthLevel(int node) {
		int level = 0;
		node = node + 1;
		while (node > 0) {
			level++;
			node >>= 1;
		}
		return level-1;
	}
	
	protected int computeBlockNumber(byte[] digest, byte[] blocks) {
		int current = 0;
		while (bitwiseOperations.getBit(blocks, current)) {
			// This block has split, need to work out the new block based on the depth of this block in the tree
			current = 2*current + (bitwiseOperations.getBit(digest, depthLevel(current)) ? 2 : 1);
		}
		return current;
	}
	
	private Key getCombinedKey(String recordKey, int blockNum) {
		return new Key(namespace, setName, blockNum > 0 ? recordKey + ":" + blockNum : recordKey);
	}
	
	/**
	 * Convenience method to extract the contents of a set of records and put them into a map.
	 */
	private TreeMap<Object, Object> recordSetToMap(Set<Record> recordSet) {
		TreeMap<Object, Object> map = new TreeMap<>();
		if (recordSet != null) {
			for (Record record : recordSet) {
				if (record != null) {
					map.putAll(record.getMap(dataBinName));
				}
			}
		}
		return map;
	}
	
	/**
	 * Read a single key from the Map and return the value associated with it
	 * @param recordKeyValue
	 * @param mapKey
	 * @param operation
	 * @return
	 */
	private Object get(String recordKeyValue, Object mapKey, byte[] digest) {
		Key rootKey = new Key(namespace, setName, recordKeyValue);
		Value mapKeyValue;
		if (useDigestForMapKey || mapKey == null) {
			mapKeyValue = Value.get(digest == null ? getHashFunction().getHash(mapKey) : digest);
		}
		else {
			mapKeyValue = objectToValue(mapKey);
		}
		Record record = client.operate(null, rootKey, 
				MapOperation.getByKey(dataBinName, mapKeyValue, MapReturnType.VALUE),
				Operation.get(BLOCK_MAP_BIN));
		
		// Check to see if which block we should read
		byte[] bitmap = (byte[])record.getValue(BLOCK_MAP_BIN);
		if (bitwiseOperations.getBit(bitmap, 0)) {
			// This block has split, we need to work out the correct block and re-read.
			if (digest == null) {
				// We must have a digest now
				digest = hashFunction.getHash(mapKey);
			}
			int block = computeBlockNumber(digest, bitmap);
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
	
	public Object get(String recordKeyValue, int mapKey) {
		return get(recordKeyValue, mapKey, null);
	}
	
	public Object get(String recordKeyValue, long mapKey) {
		return get(recordKeyValue, mapKey, null);
	}
	
	public Object get(String recordKeyValue, String mapKey) {
		return get(recordKeyValue, mapKey, null);
	}
	
	public Object get(String recordKeyValue, byte[] digest) {
		return get(recordKeyValue, null, digest);
	}
	
	/**
	 * Get all of the records associated with the passed keyValue. The result will be a TreeMap (ordered map by key) which contains all the records in the adaptive map.
	 */
	@Override
	public TreeMap<Object, Object> getAll(Policy readPolicy, String keyValue) {
		Key rootKey = new Key(namespace, setName, keyValue);
		Set<Record> records = new HashSet<>();
		Record result = client.get(readPolicy, rootKey);

		if (result != null) {
			byte[] bitmap = (byte[]) result.getValue(BLOCK_MAP_BIN);
			if (bitwiseOperations.getBit(bitmap, 0)) {
				
				// This block has been split, the results are in the sub-blocks
				List<Integer> blockList = new ArrayList<>();
				computeBlocks(bitmap, 0, blockList);
				Set<Integer> blocksInDoubt = new HashSet<>();
				Set<Integer> blocksToMarkAsExpired = null;
				while (blockList.size() > 0)  {
					// The blocks to be read are now in blockList. This give an index
					Key[] keys = new Key[blockList.size()];
					int count = 0;
					for (Integer thisBlock : blockList) {
						keys[count++] = getCombinedKey(keyValue, thisBlock);
					}
					BatchPolicy batchPolicy = new BatchPolicy();
					batchPolicy.maxConcurrentThreads = 0;
					Record[] results = client.get(batchPolicy, keys);
					for (count = 0; count < results.length; count++) {
						if (results[count] == null) {
							// This has potentially split
							blocksInDoubt.add(blockList.get(count));
						}
						else {
							records.add(results[count]);
						}
					}
					
					blockList.clear();
					if (blocksInDoubt.size() > 0) {
						// Re-read the root record to determine what's happened. This should be very rare: (a block TTLd out, or a block split 
						// between the time we read the original bitmap and when we read this record)
						Key key = new Key(namespace, setName, keyValue);
						Record rootRecord = client.get(batchPolicy, key, BLOCK_MAP_BIN);
						if (rootRecord != null) {
							byte[] blocks = (byte [])rootRecord.getValue(BLOCK_MAP_BIN);
							for (Integer blockNum : blocksInDoubt) {
								if (bitwiseOperations.getBit(blocks, blockNum)) {
									// This has split, add it to the next read. Don't update the block list
									// as this is the job of the writing process.
									blockList.add(2*blockNum+1);
									blockList.add(2*blockNum+2);
								}
								else {
									// This record has TTLd out. We should update the bitmap async.
									if (blocksToMarkAsExpired == null) {
										blocksToMarkAsExpired = new HashSet<>();
									}
									blocksToMarkAsExpired.add(blockNum);
								}
							}
						}
					}
				}
				
			}
			else {
				// This block exists and has not been split, therefore it contains the results.
				records.add(result);
			}
			return recordSetToMap(records);
		}
		else {
			return null;
		}
	}

	/**
	 * Get a count of all of the records associated with the passed keyValue.
	 */
	@Override
	public int countAll(WritePolicy policy, String keyValue) {
		// TODO: refactor this to the Async API.
		Key rootKey = new Key(namespace, setName, keyValue);
		Record result = client.operate(policy, rootKey, Operation.get(BLOCK_MAP_BIN), MapOperation.getByIndexRange(this.dataBinName, 0, MapReturnType.COUNT));

		if (result != null) {
			byte[] bitmap = (byte[]) result.getValue(BLOCK_MAP_BIN);
			if (bitwiseOperations.getBit(bitmap, 0)) {

				int count = 0;
				// This block has been split, the results are in the sub-blocks
				List<Integer> blockList = new ArrayList<>();
				computeBlocks(bitmap, 0, blockList);
				Set<Integer> blocksInDoubt = new HashSet<>();
				
				while (blockList.size() > 0)  {
					for (int thisBlock : blockList) {
						Record mapCount = client.operate(null, getCombinedKey(keyValue, thisBlock), MapOperation.getByIndexRange(this.dataBinName, 0, MapReturnType.COUNT));
						
						if (mapCount == null) {
							// This has potentially split
							blocksInDoubt.add(blockList.get(count));
						}
						else {
							count += mapCount.getInt(this.dataBinName);
						}
					}
					
					blockList.clear();
					if (blocksInDoubt.size() > 0) {
						// Re-read the root record to determine what's happened. This should be very rare: (a block TTLd out, or a block split 
						// between the time we read the original bitmap and when we read this record)
						Key key = new Key(namespace, setName, keyValue);
						Record rootRecord = client.get(null, key, BLOCK_MAP_BIN);
						if (rootRecord != null) {
							byte[] blocks = (byte [])rootRecord.getValue(BLOCK_MAP_BIN);
							for (Integer blockNum : blocksInDoubt) {
								if (bitwiseOperations.getBit(blocks, blockNum)) {
									// This has split, add it to the next read. Don't update the block list
									// as this is the job of the writing process.
									blockList.add(2*blockNum+1);
									blockList.add(2*blockNum+2);
								}
							}
						}
					}
				}
				return count;
				
			}
			else {
				// This block exists and has not been split, therefore it contains the results.
				return result.getInt(this.dataBinName);
			}
		}
		return 0;
	}
	
	private static class KeyBlockContainer {
		String keyValue;
		int block;
		public KeyBlockContainer(String keyValue, int block) {
			super();
			this.keyValue = keyValue;
			this.block = block;
		}
	}
	
	/**
	 * Get all of the records associated with all of the keys passed in. The returned records will be in the same
	 * order as the input recordKeyValues. 
	 */
	@Override
	public TreeMap<Object, Object>[] getAll(BatchPolicy batchPolicy, String[] recordKeyValues) {
		final int count = recordKeyValues.length;
		@SuppressWarnings("unchecked")
		final TreeMap<Object, Object>[] resultsAsTreeMap = new TreeMap[count];

		
		@SuppressWarnings("unchecked")
		Set<Record>[] recordSet = new Set[count];

		Key[] rootKeys = new Key[count];
		for (int i = 0; i < count; i++) {
			rootKeys[i] = new Key(namespace, setName, recordKeyValues[i]);
		}
		if (batchPolicy == null) {
			batchPolicy = new BatchPolicy();
		}
		batchPolicy.maxConcurrentThreads = 0;

		Record[] batchResults = client.get(batchPolicy, rootKeys);

		List<Integer> batchSplitOrigins = null;
		List<Integer> batchBlockList = null;
		List<Key> batchBlockKeyList = null;
//		Set<Integer> blocksInDoubt = null;
//		Set<Integer> blocksToMarkAsExpired = null;
		for (int i = 0; i < count; i++) {
			Record thisRecord = batchResults[i];
			if (thisRecord != null) {
				byte[] bitmap = (byte[]) thisRecord.getValue(BLOCK_MAP_BIN);
				if (bitwiseOperations.getBit(bitmap, 0)) {
				
					// This block has been split, the results are in the sub-blocks
					if (batchSplitOrigins == null) {
						batchSplitOrigins = new ArrayList<>();
						batchBlockList = new ArrayList<>();
						batchBlockKeyList = new ArrayList<>();
//						blocksInDoubt = new HashSet<>();
					}
					List<Integer> blockList = new ArrayList<>();
					computeBlocks(bitmap, 0, blockList);

					// The blocks to be read are now in blockList. This give an index
					for (int thisBlock : blockList) {
						batchBlockList.add(thisBlock);
						batchBlockKeyList.add(getCombinedKey(recordKeyValues[i], thisBlock));
						batchSplitOrigins.add(i);
					}
				}
				else {
					// Just add this to the final record set
					resultsAsTreeMap[i] = (TreeMap<Object, Object>) thisRecord.getMap(dataBinName);
				}
			}
		}
		
		while (batchSplitOrigins != null && !batchSplitOrigins.isEmpty()) {
			// Now batch read the items in the list
			Record[] batchGetResults = client.get(batchPolicy, batchBlockKeyList.toArray(new Key[batchBlockKeyList.size()]));
			
			List<Integer> newBatchSplitOrigins = null;
			batchBlockKeyList.clear();
			List<KeyBlockContainer> containers = null;
			Set<String> keySet = null;
			
			for (int i = 0; i < batchGetResults.length; i++) {
				if (batchGetResults[i] != null) {
					// These are continuation results, we must put them back in the set of results associated with
					// this request. The mapping of the originating request is stored in the batchSplitOrigins
					int index = batchSplitOrigins.get(i);
					if (recordSet[index] == null) {
						recordSet[index] = new HashSet<>();
					}
					recordSet[index].add(batchGetResults[i]);
				}
				else {
					// This block might have split whilst we were reading it. We need to re-read all the root blocks where this has happened
					if (containers == null) {
						containers = new ArrayList<>();
						keySet = new HashSet<>();
					}
					int index = batchSplitOrigins.get(i);
					containers.add(new KeyBlockContainer(recordKeyValues[index], batchBlockList.get(i)));
					keySet.add(recordKeyValues[index]);
				}
			}
			if (containers != null) {
				// TODO: Cover this case
				/*
				Key[] newRootKeys = new Key[keySet.size()];
				int subCount = 0;
				String[] keys = keySet.toArray(new String[keySet.size()]);
				for (String thisKey : keys) {
					newRootKeys[subCount++] = new Key(namespace, setName, thisKey);
				}
				batchResults = client.get(batchPolicy, rootKeys);
				
				Map<String, byte[]> blockMaps = new HashMap<>();
				// Unpack the results out
				for (int i = 0; i < subCount; i++) {
					blockMaps.put(keySet., value)
				}
				*/
			}

			batchSplitOrigins = newBatchSplitOrigins;
		}				
		
/*							
							
					blockList.clear();
					if (blocksInDoubt.size() > 0) {
						// Re-read the root record to determine what's happened. This should be very rare: (a block TTLd out, or a block split 
						// between the time we read the original bitmap and when we read this record)
						Key key = new Key(namespace, setName, keyValue);
						Record rootRecord = client.get(null, key, BLOCK_MAP_BIN);
						if (rootRecord != null) {
							byte[] blocks = (byte [])rootRecord.getValue(BLOCK_MAP_BIN);
							for (Integer blockNum : blocksInDoubt) {
								if (bitwiseOperations.getBit(blocks, blockNum)) {
									// This has split, add it to the next read. Don't update the block list
									// as this is the job of the writing process.
									blockList.add(2*blockNum+1);
									blockList.add(2*blockNum+2);
								}
								else {
									// This record has TTLd out. We should update the bitmap async.
									if (blocksToMarkAsExpired == null) {
										blocksToMarkAsExpired = new HashSet<>();
									}
									blocksToMarkAsExpired.add(blockNum);
								}
							}
						}
					}
				}
				
			}
			else {
				// This block exists and has not been split, therefore it contains the results.
				records.add(result);
			}
		}
		return records;
		*/
		for (int i = 0; i < count; i++) {
			if (recordSet[i] != null) {
				resultsAsTreeMap[i] = recordSetToMap(recordSet[i]);
			}
		}
		return resultsAsTreeMap;

	}

	/**
	 * Get all the records which match the passed operations. The operations are able to do things like "getByValueRange", etc. Each operation
	 * will be applied to all the records which match, and the records returned.
	 */
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
			byte[] bitmap = (byte[]) result.getValue(BLOCK_MAP_BIN);
			if (bitwiseOperations.getBit(bitmap, 0)) {
				
				// This block has been split, the results are in the sub-blocks
				List<Integer> blockList = new ArrayList<>();
				computeBlocks(bitmap, 0, blockList);
				Set<Integer> blocksInDoubt = new HashSet<>();
				Set<Integer> blocksToMarkAsExpired = null;
				// The blocks to be read are now in blockList. This give an index
				while (blockList.size() > 0)  {
					blockList.parallelStream().forEach(item -> {
						Key thisKey = getCombinedKey(keyValue, item);
						Record thisResult = client.operate(opPolicy, thisKey, operations);
						if (thisResult != null) {
							records.add(thisResult);
						}
						else {
							// There are 2 reasons this might have returned null: either the block has split and been removed whilst we were processing the read
							// or the record has TTLd out. We need to check the root block to check.
							blocksInDoubt.add(item);
						}
					});
					blockList.clear();
					if (blocksInDoubt.size() > 0) {
						// Re-read the root record to determine what's happened. This should be very rare: (a block TTLd out, or a block split 
						// between the time we read the original bitmap and when we read this record)
						Key key = new Key(namespace, setName, keyValue);
						Record rootRecord = client.get(opPolicy, key, BLOCK_MAP_BIN);
						if (rootRecord != null) {
							byte[] blocks = (byte [])rootRecord.getValue(BLOCK_MAP_BIN);
							for (Integer blockNum : blocksInDoubt) {
								if (bitwiseOperations.getBit(blocks, blockNum)) {
									// This has split, add it to the next read. Don't update the block list
									// as this is the job of the writing process.
									blockList.add(2*blockNum+1);
									blockList.add(2*blockNum+2);
								}
								else {
									// This record has TTLd out. We should update the bitmap async.
									if (blocksToMarkAsExpired == null) {
										blocksToMarkAsExpired = new HashSet<>();
									}
									blocksToMarkAsExpired.add(blockNum);
								}
							}
						}
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
	public Object delete(WritePolicy writePolicy, String recordKeyValue, Object mapKey, byte[] digest) {
		Value mapKeyValue;
		if (useDigestForMapKey || mapKey == null) {
			mapKeyValue = Value.get(digest == null ? getHashFunction().getHash(mapKey) : digest);
		}
		else {
			mapKeyValue = objectToValue(mapKey);
		}
		String id = getLockId();
		Operation obtainLock = getObtainLockOperation(id, 0);
		Operation removeFromMap = MapOperation.removeByKey(dataBinName, mapKeyValue, MapReturnType.VALUE);
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
					Record record = waitForRootBlockToFullyLock(LockType.SUBDIVIDE_BLOCK, key, MAX_LOCK_TIME);
					if (record == null) {
						// The lock vanished from under us, maybe it TTLd out, just try again.
						continue;
					}
					else {
						// Check to see which block we should read
						byte[] bitmap = (byte[])record.getValue(BLOCK_MAP_BIN);
						if (digest == null) {
							// We must have a digest now
							digest = hashFunction.getHash(mapKey);
						}
						int block = computeBlockNumber(digest, bitmap);
			
						try {
							WritePolicy writePolicy2 = writePolicy == null ? new WritePolicy() : new WritePolicy(writePolicy);
							writePolicy2.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
							record = client.operate(writePolicy2, getCombinedKey(recordKeyValue, block),
									obtainLock,
									MapOperation.removeByKey(dataBinName, mapKeyValue, MapReturnType.VALUE),
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
	
	public Object delete(WritePolicy writePolicy, String recordKeyValue, int mapKey) {
		return delete(writePolicy, recordKeyValue, mapKey, null);
	}
	
	public Object delete(WritePolicy writePolicy, String recordKeyValue, long mapKey) {
		return delete(writePolicy, recordKeyValue, mapKey, null);
	}
	
	public Object delete(WritePolicy writePolicy, String recordKeyValue, String mapKey) {
		return delete(writePolicy, recordKeyValue, mapKey, null);
	}
	
	public Object delete(WritePolicy writePolicy, String recordKeyValue, byte[] digest) {
		return delete(writePolicy, recordKeyValue, null, digest);
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
					Record record = waitForRootBlockToFullyLock(LockType.SUBDIVIDE_BLOCK, key, MAX_LOCK_TIME);
					if (record == null) {
						// The lock vanished from under us, maybe it TTLd out, just try again.
						continue;
					}
					else {
						// Check to see which block we should read
						byte[] bitmap = (byte[])record.getValue(BLOCK_MAP_BIN);
						if (digest == null) {
							// We must have a digest now
							digest = hashFunction.getHash(mapKey);
						}
						int block = computeBlockNumber(digest, bitmap);
			
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
	private Record waitForRootBlockToFullyLock(LockType lockType, Key key, long timeoutInMs) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
		while (true) {
			try {
				Record record = client.get(null, key, BLOCK_MAP_BIN, lockType.getBinName());
				if (record == null) {
					return null;
				}
				byte[] blocks = (byte[]) record.getValue(BLOCK_MAP_BIN);
				Map<String, List<Object>> lockData = (Map<String, List<Object>>) record.getMap(lockType.getBinName());
				if (lockData == null || lockData.isEmpty()) {
					// The block is not locked. This is not expected, so return null
					return null;
				}
				else {
					// The lock is held, see if the block has split yet.
					if (bitwiseOperations.getBit(blocks, 0)) {
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
	 * gets locked when it's splitting, so it is expected that the first bit of the block map will be 1. If this is the
	 * case, return. If the block is locked but the first bit of the block map is not 1, wait for it to become 1 or
	 * the lock to expire. If the lock expires, assume the writing process has died, acquire the lock ourselves and perform the split.
	 * 
	 * @param key
	 * @param timeoutInMs
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Record waitForRootBlockLock(LockType lockType, Key key, long timeoutInMs) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
		while (true) {
			try {
				Record record = client.get(null, key);
				if (record == null) {
					return null;
				}
				byte[] blocks = (byte[]) record.getValue(BLOCK_MAP_BIN);
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
					if (bitwiseOperations.getBit(blocks, 0)) {
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

	private Record waitForLockRelease(LockType lockType, Key key, long timeoutInMs) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
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
	private Record acquireLock(LockType lockType, Key key, long timeoutInMs, boolean getData) {
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
					Record record = client.operate(wp, key, getObtainLockOperation(id, now), removeUnlockedTagOperation(), Operation.get(BLOCK_MAP_BIN), dataOperation);
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
						record = client.get(null, key, lockType.getBinName(), BLOCK_MAP_BIN, dataBinName);
					}
					else {
						record = client.get(null, key, lockType.getBinName(), BLOCK_MAP_BIN);
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
	 * Note that at the moment 
	 * @param recordKey - The key of the record on which the lock is to be released 
	 * @return -  true if this thread owned the lock and the lock was successfully released, false if this thread did not own the lock.
	 */
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
	 * @param mapKeyDigest
	 * @param dataInsertOp
	 * @return true if the operation was successful, false if the operation failed but it is a recoverable error where retrying may succeed.
	 */
	private boolean splitBlockAndInsert(WritePolicy writePolicy, String recordKey, int blockNum, Object mapKey, byte[] mapKeyDigest, Object dataValue, byte[] blockMap, int blockMapGeneration) {
		return splitBlockAndInsert(writePolicy, recordKey, blockNum, mapKey, mapKeyDigest, dataValue, blockMap, blockMapGeneration, null, 0);
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
	 * @param mapKeyDigest - The digest of the map key. If this parameter is null, the <code>hashFunction</code> will be used to compute it.
	 * @param dataValue - The value to insert into the map.
	 * @param blockMap - The current block map. Can be <code>null</code>, which will force a database read.
	 * @param blockMapGeneration - The generation of the block map. If this value is < 0 the blockMap parameter will be ignored and the value re-read from the database
	 * @param data - The data of the record. If this is <code>null</code> it will be read from the database 
	 * @param dataTTL - the TTL of this subblock. If the <code>data</code> parameter above is <code>null</code>, this will be read from the record along with the data
	 * 					and the passed value ignored.
	 * @return <code>true</code> if the operation was successful, <code>false</code> if the operation failed but could conceivably succeed on a retry, eg a race condition.
	 */
	@SuppressWarnings("unchecked")
	private boolean splitBlockAndInsert(WritePolicy writePolicy, String recordKey, int blockNum, Object mapKey, byte[] mapKeyDigest, Object dataValue, byte[] blockMap, int blockMapGeneration, Map<Object, Object> data, int dataTTL) {
		// We will always need a digest here
		if (mapKeyDigest == null) {
			mapKeyDigest = hashFunction.getHash(mapKey);
		}

		// We must have a valid block map, it may or may not have been provided. Note: If the map is old, it does not matter. The only valid state
		// transitions for a block in the map is for the bit to go from 0->1 when the block splits. It can never go back to 0, and "unsplit".
		// Note that the block map can be null. If the root block has not split, we will validly have a record with a positive generation and 
		// a null block map
		if (blockMapGeneration < 0) {
			Record record = client.get(null, getCombinedKey(recordKey, 0), BLOCK_MAP_BIN);
			if (record != null) {
				blockMap = (byte[])record.getValue(BLOCK_MAP_BIN);
				blockMapGeneration = record.generation;
			}
			else {
				// The root block has gone away! This is unexpected, since to get here we must have been able to read it earlier in this operation.
				throw new RootBlockRemoved("Root block removed whilst splitting block " + blockNum  + " on record " + recordKey);
			}
		}

		// Mark this block as split in the map
		blockMap = bitwiseOperations.setBit(blockMap, blockNum);
		
		// We want to get the lock and retrieve all the data in one hit
		Key key = getCombinedKey(recordKey, blockNum);
		if (data == null) {
			Record currentRecord;
			try {
				currentRecord = acquireLock(LockType.SUBDIVIDE_BLOCK, key, MAX_LOCK_TIME, true);
			}
			catch (RecordDoesNotExistException rdnee) {
				// The lock could not be obtained becasue the underlying record has been removed. THis could be because another 
				// thread already split it. Retry the insert again
				return false;
			}
			// It is possible that the block has been split by another thread since we determined we needed to split it. Check the bit in the bitmap
			byte[] newBlockMap = (byte[])currentRecord.getValue(BLOCK_MAP_BIN);
			if (bitwiseOperations.getBit(newBlockMap, blockNum)) {
				return false;
			}
			data = (Map<Object, Object>)currentRecord.getMap(dataBinName);
			dataTTL = currentRecord.getTimeToLive();
			if (blockNum == 0) {
				// We're locking the root block which does a write, hence we must update the generation counter
				blockMapGeneration = currentRecord.generation;
			}
		}
		
		// Add in the current record under consideration
		if (useDigestForMapKey) {
			data.put(ByteBuffer.wrap(mapKeyDigest), dataValue);
		}
		else {
			data.put(mapKey, dataValue);
		}

		// Create the 2 halves of the map and populate them. We must create both halves even if all the records happen to fall into 
		// one half, so future records which expect to use that block find it present. 
		Map<Integer, Map<Value, Value>> dataHalves = new HashMap<>();
		dataHalves.put(2*blockNum+1, new HashMap<>(data.size() *2/3));
		dataHalves.put(2*blockNum+2, new HashMap<>(data.size() *2/3));

		for (Object thisKey : data.keySet()) {
			Object value = data.get(thisKey);
			byte[] digest;
			Object keyToUse = thisKey;
			if (useDigestForMapKey) {
				// The key is already the digest, we can just split on it.
				// The data is either in a byte[] if using an unordered map or a ByteBuffer() if using a k- or kv-ordered map
				if (thisKey instanceof ByteBuffer) {
					digest = ((ByteBuffer)thisKey).array();
				}
				else {
					digest = (byte [])thisKey;
				}
				keyToUse = digest;
			}
			else {
				// The key is some other value, we need to re-hash it to work out where to split. 
				digest = hashFunction.getHash(thisKey);
			}
			int thisHalfBlockNum = computeBlockNumber(digest, blockMap);
//			if (!dataHalves.containsKey(thisHalfBlockNum)) {
//				dataHalves.put(thisHalfBlockNum, new HashMap<>(data.size() *2/3));
//			}
			dataHalves.get(thisHalfBlockNum).put(Value.get(keyToUse), Value.get(value));
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

		for (Integer newBlockNum : dataHalves.keySet()) {
			// Note: use the map operations to insert the data, otherwise it's possible to get duplicate keys in maps
			//client.put(wp, getCombinedKey(recordKey, newBlockNum), new Bin(dataBinName, dataHalves.get(newBlockNum)));
			client.operate(writePolicy, getCombinedKey(recordKey, newBlockNum), 
					MapOperation.putItems(mapPolicy, dataBinName, dataHalves.get(newBlockNum)), getUnlockedTagOperation());
		}
		
		// The sub-records now exist, update the bitmap and delete this record
		updateRootBlockBitmap(recordKey, blockNum, blockMap, blockMapGeneration);
		if (blockNum > 0) {
			// This does not need to be done durably. If it comes back it will just TTL later
			// TODO: What to do if the TTL is 0?
			if (forceDurableDeletes) {
				writePolicy.durableDelete = true;
			}
			client.delete(writePolicy, key);
		}
		return true;
	}
	
	
	private void updateRootBlockBitmap(String recordKey, int blockNum, byte[] blockMap, int blockMapGeneration) {
		WritePolicy writePolicy = new WritePolicy();
		Key key = getCombinedKey(recordKey, 0);
		while (true)  {
			writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			writePolicy.generation = blockMapGeneration;
			writePolicy.sendKey = this.sendKey;

			try {
				if (blockNum == 0) {
					// The root block has split, remove the data from the root block. Do NOT release the lock as this is a flag to imply we've split.
					client.put(writePolicy, key, new Bin(BLOCK_MAP_BIN, Value.get(blockMap)), Bin.asNull(dataBinName));
				}
				else {
					client.put(writePolicy, key, new Bin(BLOCK_MAP_BIN, Value.get(blockMap)));
				}
				break;
			}
			catch (AerospikeException ae) {
				switch (ae.getResultCode()) {
					case ResultCode.GENERATION_ERROR:
						// Another thread has updated this record, which must be because another block has split, re-read
						Record newRecord = client.get(null, key, BLOCK_MAP_BIN);
						blockMap = (byte[]) newRecord.getValue(BLOCK_MAP_BIN);
						blockMap = bitwiseOperations.setBit(blockMap, blockNum);
						blockMapGeneration = newRecord.generation;
						break;
						
					default:
						throw ae;
				}
						
			}
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
	private boolean putToSubBlock(WritePolicy writePolicy, String recordKey, int blockNum, Object mapKey, byte[] mapKeyDigest, Value value, byte[] blockMap, int blockMapGeneration) {
		if (writePolicy == null) {
			writePolicy = new WritePolicy();
		}
		writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		
		// We want to insert into the map iff the record is not locked. Hence, we will write the lock with CREATE_ONLY, insert
		// the record, then delete the lock. If the operation succeeds, we know the lock was not held during the write
		// Note that as we never actually hold the lock, we don't care about the lock time value
		String id = getLockId();
		Operation obtainLock = getObtainLockOperation(id, 0);
		Operation addToMap = MapOperation.put(mapPolicy, dataBinName, Value.get(useDigestForMapKey ? mapKeyDigest : mapKey), value);
		Operation releaseLock = getReleaseLockOperation(id);

		Key key = getCombinedKey(recordKey, blockNum);

		try {
			// TODO: Determine whether to do the read here (high network, low chance of being needed) or 
			// do a second read if needed (higher IOPS, but with a decent PWQ should be ok, but extra round-trip)
			Record result = client.operate(writePolicy, key, obtainLock, addToMap, releaseLock);
			int recordsInBlock = result.getInt(dataBinName);
			if (recordsInBlock > this.recordThreshold) {
				return splitBlockAndInsert(writePolicy, recordKey, blockNum, mapKey, mapKeyDigest, value.getObject(), blockMap, blockMapGeneration);
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
					return splitBlockAndInsert(writePolicy, recordKey, blockNum, mapKey, mapKeyDigest, value.getObject(), blockMap, blockMapGeneration);
					
				default:
					throw ae;
			}
		}
	}
	
	/**
	 * Put a record to the adaptive map
	 * @param recordKey - the key to the record to use
	 * @param mapKey = the key to insert into the map
	 * @param mapKeyDigest - the digest encoding of the mapKey. If this is null, it will be determined when needed
	 * @param value - the value associate with the key.
	 */
	public void put(WritePolicy writePolicy, String recordKey, Object mapKey, byte[] mapKeyDigest, Value value) {
		if (useDigestForMapKey && mapKey != null && mapKeyDigest == null) {
			mapKeyDigest = hashFunction.getHash(mapKey);
		}
		
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
		Operation addToMap = MapOperation.put(mapPolicy, dataBinName, Value.get(useDigestForMapKey ? mapKeyDigest : mapKey), value);
		Operation releaseLock = getReleaseLockOperation(id);
		Operation getBlockMap = Operation.get(BLOCK_MAP_BIN);

		Key key = getCombinedKey(recordKey, 0);

		try {
			while (true) {
				Record result = client.operate(writePolicy, key, obtainLock, addToMap, getBlockMap, releaseLock);
				int recordsInBlock = result.getInt(dataBinName);
				if (recordsInBlock > this.recordThreshold) {
					// TODO: Again, is it better to read the whole record whilst we have it, or re-read it
					if (splitBlockAndInsert(writePolicy, recordKey, 0, mapKey, mapKeyDigest, value.getObject(), (byte[])result.getValue(BLOCK_MAP_BIN), result.generation)) {
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
					Record record = waitForRootBlockLock(LockType.SUBDIVIDE_BLOCK, key, MAX_LOCK_TIME);
					if (record == null) {
						// The block vanished from under us, maybe it TTLd out, just try again. Note this is a VERY unusual case, so recursion here should
						// be ok and not cause any stack overflow
						put(writePolicy, recordKey, mapKey, mapKeyDigest, value);
						break;
					}
					else {
						// Either the block has split or we must split it.
						byte[] blockMap = (byte[]) record.getValue(BLOCK_MAP_BIN);
						if (bitwiseOperations.getBit(blockMap, 0)) {
							// The block has already split, we need to update the appropriate sub-block
							if (mapKeyDigest == null) {
								mapKeyDigest = hashFunction.getHash(mapKey);
							}
							int blockToAddTo = computeBlockNumber(mapKeyDigest, blockMap);
							if (putToSubBlock(writePolicy, recordKey, blockToAddTo, mapKey, mapKeyDigest, value, blockMap, record.generation)) {
								break;
							}
						}
						else {
							// We own the lock, but must split this block
							if (splitBlockAndInsert(writePolicy, recordKey, 0, mapKey, mapKeyDigest, value.getObject(), blockMap, record.generation, (Map<Object,Object>)record.getMap(dataBinName), record.getTimeToLive())) {
								break;
							}
						}
					}
				}
			}
			else if (ae.getResultCode() == ResultCode.RECORD_TOO_BIG) {
				splitBlockAndInsert(writePolicy, recordKey, 0, mapKey, mapKeyDigest, value.getObject(), null, -1);
			}
			else {
				throw ae;
			}
		}	
		/*
		
		Key key = getCombinedKey(recordKey, blockToAddTo);

		// If this is not the root block, the block is created already
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.recordExistsAction = (blockToAddTo == 0) ? RecordExistsAction.UPDATE : RecordExistsAction.UPDATE_ONLY;
		
		// We want to insert into the map iff the record is not locked. Hence, we will write the lock with CREATE_ONLY, insert
		// the record, then delete the lock. If the operation succeeds, we know the lock was not held during the write
		// Note that as we never actually hold the lock, we don't care about the lock time value
		String id = getLockId();
		Operation obtainLock = getObtainLockOperation(id, 0);
		Operation addToMap = MapOperation.put(mapPolicy, DATA_BIN, Value.get(useDigestForMapKey ? mapKeyDigest : mapKey), value);
		Operation releaseLock = getReleaseLockOperation(id);

		try {
			Record result = client.operate(writePolicy, key, obtainLock, addToMap, releaseLock);
			System.out.println(result.getInt(DATA_BIN));
		}
		catch (Exception ae) {
			// TODO: Implement block splitting logic
			throw ae;
		}
		*/
	}
	
	public void deleteAll(WritePolicy writePolicy, String recordKeyValue) {
		Key key = getCombinedKey(recordKeyValue, 0);
		Record record = client.get(null, key);
		if (record != null) {
			
			if (forceDurableDeletes) {
				writePolicy.durableDelete = true;
			}
			client.delete(writePolicy, key);
			// Now delete all the sub-blocks.
			byte[] bitmap = (byte[]) record.getValue(BLOCK_MAP_BIN);

			List<Integer> blockList = new ArrayList<>();
			computeBlocks(bitmap, 0, blockList);
			
			writePolicy.maxRetries = 5;
			writePolicy.totalTimeout = 5000;
			writePolicy.sleepBetweenRetries = 250;
			for (int block : blockList) {
				if (block > 0) {
					client.delete(writePolicy, getCombinedKey(recordKeyValue, block));
				}
			}

		}
	}

}
