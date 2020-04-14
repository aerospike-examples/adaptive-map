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

import java.util.Set;
import java.util.TreeMap;

import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public interface IAdaptiveMap {
	/** 
	 * Return the hash function used by this map. If {@link #setHashFuction(Hash)} has not been called, this will 
	 * return a hash which does RIPEMD160 encoding
	 * @return
	 */
	public Hash getHashFunction();
	/**
	 * 
	 * @param hash
	 */
	public void setHashFuction(Hash hash);
	
	/**
	 * Remove all the sub-records in the adaptive map
	 * @param recordKeyValue
	 */
	public void deleteAll(String recordKeyValue);
	
	/**
	 * Remove a single record from the underlying map
	 * @param recordKeyValue - the key of the record
	 * @param mapKey - the key to remove from the map 
	 */
	public Object delete(String recordKeyValue, int mapKey);
	public Object delete(String recordKeyValue, long mapKey);
	public Object delete(String recordKeyValue, String mapKey);
	public Object delete(String recordKeyValue, byte[] digest);
	
	/**
	 * Insert / Update a value in the adadptive map
	 * @param recordKey - the key of the record 
	 * @param mapKey - the key of the map to insert / update.
	 * @param mapKeyDigest - the digest of the key. Can be null. If null and it is needed, it will be obtained by invoking the getHashFunction().getHash(mapKey)
	 * @param value - The value to be stored in the map
	 */
	public void put(String recordKey, Object mapKey, byte[] mapKeyDigest, Value value);
	
	/**
	 * Perform all the operations passed to this method on every map/sub-map associated with the passed key. The operations may be applied on 
	 * the sub-maps in parallel. 
	 * @param opPolicy - The WritePolicy to be passed to the individual operations
	 * @param keyValue - The key of the map
	 * @param operations - The set of operations to be applied to each map/sub-map
	 * @return - The set of records associated with the applied operations. There will be one record for each map/sub-map in the set.
	 */
	public Set<Record> getAll(WritePolicy opPolicy, String keyValue, Operation ... operations);
	/**
	 * Get all of the records associated with the passed keyValue. The result will be a TreeMap (ordered map by key) which contains all the records in the adaptive map.
	 * @param readPolicy
	 * @param keyValue
	 * @return
	 */
	public TreeMap<Object, Object> getAll(Policy readPolicy, String keyValue);
	public TreeMap<Object, Object>[] getAll(BatchPolicy batchPolicy, String []keyValues);
	
	public Object get(String recordKeyValue, int mapKey);
	public Object get(String recordKeyValue, long mapKey);
	public Object get(String recordKeyValue, String mapKey);
	public Object get(String recordKeyValue, byte[] digest);
}
