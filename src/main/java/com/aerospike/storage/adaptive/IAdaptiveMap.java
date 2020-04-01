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
	
	public void put(String recordKey, Object mapKey, byte[] mapKeyDigest, Value value);
	public Set<Record> getAll(WritePolicy opPolicy, String keyValue, Operation ... operations);
	public Set<Record> getAll(Policy readPolicy, String keyValue);
	public Set<Record>[] getAll(BatchPolicy batchPolicy, String []keyValues);
	public Object get(String recordKeyValue, int mapKey);
	public Object get(String recordKeyValue, long mapKey);
	public Object get(String recordKeyValue, String mapKey);
	public Object get(String recordKeyValue, byte[] digest);
}
