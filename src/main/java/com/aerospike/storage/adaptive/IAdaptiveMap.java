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
	public void deleteAll(WritePolicy writePolicy, String recordKeyValue);
	
	/**
	 * Remove a single record from the underlying map
	 * @param recordKeyValue - the key of the record
	 * @param mapKey - the key to remove from the map 
	 */
	public Object delete(WritePolicy writePolicy, String recordKeyValue, int mapKey);
	public Object delete(WritePolicy writePolicy, String recordKeyValue, long mapKey);
	public Object delete(WritePolicy writePolicy, String recordKeyValue, String mapKey);
	public Object delete(WritePolicy writePolicy, String recordKeyValue, byte[] digest);
	
	/**
	 * Insert / Update a value in the adaptive map
	 * @param writePolicy - the write policy to use for retries, etc. Some of the parameters like the RecordExistsAction will be overridden as necessary
	 * @param recordKey - the key of the record 
	 * @param mapKey - the key of the map to insert / update.
	 * @param mapKeyDigest - the digest of the key. Can be null. If null and it is needed, it will be obtained by invoking the getHashFunction().getHash(mapKey)
	 * @param value - The value to be stored in the map
	 */
	public void put(WritePolicy writePolicy, String recordKey, Object mapKey, byte[] mapKeyDigest, Value value);
	
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
	/**
	 * Get a count of all of the records associated with the passed keyValue.
	 */
	int countAll(WritePolicy policy, String keyValue);
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
	Object executeUdfOnRecord(WritePolicy writePolicy, String recordKeyValue, Object mapKey, byte[] digest,
			String packageName, String functionName, Value[] args);
}
