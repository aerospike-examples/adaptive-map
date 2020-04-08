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

import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public interface IAdaptiveMap {
	public Hash getHashFunction();
	public void setHashFuction(Hash hash);
	
	/**
	 * Remove all the sub-records in the adaptive map
	 * @param recordKeyValue
	 */
	public void deleteAll(String recordKeyValue);
	
	/**
	 * Remove a single record from the underlying map
	 */
	public Object delete(String recordKey, Object mapKey, byte[] mapKeyDigest);
	
	public void put(String recordKey, Object mapKey, byte[] mapKeyDigest, Value value);
	public Set<Record> getAll(WritePolicy opPolicy, String keyValue, Operation ... operations);
	public Set<Record> getAll(Policy readPolicy, String keyValue);
	public Set<Record>[] getAll(BatchPolicy batchPolicy, String []keyValues);
	public Object get(String recordKeyValue, int mapKey);
	public Object get(String recordKeyValue, long mapKey);
	public Object get(String recordKeyValue, String mapKey);
	public Object get(String recordKeyValue, byte[] digest);
}
