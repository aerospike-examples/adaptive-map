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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;

public class Lock {
	/**
	 * The maximum time any lock should be held for during the splitting of a record
	 */
	private static final long MAX_LOCK_TIME = 100;
	/**
	 * A moderately unique ID 
	 */
	private static final String ID = UUID.randomUUID().toString(); // This needs to be unique only for this session, used only for locking
	/**
	 * The default name of the bin to be used for the lock
	 */
	private static final String DEFAULT_LOCK_BIN = "lock"; 

	private final AerospikeClient client;
	private final String lockBin;
	
	public Lock(AerospikeClient client, String lockBin) {
		this.client = client;
		this.lockBin = lockBin;
	}
	
	public Lock(AerospikeClient client) {
		this(client, DEFAULT_LOCK_BIN);
	}

	private String getLockId() {
		return ID + "-" + Thread.currentThread().getId();
	}

	/**
	 * Get the operation to perform the lock. The format of the lock is:
	 * <pre>
	 *    map {
	 *       locked: [ id, timestamp_lock_expires ]
	 *    }
	 * </pre>
	 * @param id
	 * @param now
	 * @return
	 */
	private Operation getObtainLockOperation(String id, long now) {
		if (id == null) {
			id = getLockId();
		}
		List<Object> data = Arrays.asList(new Object[] { id, now + MAX_LOCK_TIME });
		MapPolicy policy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);
		return MapOperation.put(policy, lockBin, Value.get("locked"), Value.get(data));
	}
	

}
