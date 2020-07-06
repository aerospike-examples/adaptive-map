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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.command.Buffer;


public class TestAdaptiveMap {

	private AdaptiveMap library;
	private final String HOST = "127.0.0.1";
	private AerospikeClient client;
	private IAdaptiveMap adaptiveMapWithValueKey;
	private IAdaptiveMap adaptiveMapWithDigestKey;
	private final String NAMESPACE = "test";
	private final String SET = "testAdapt";
	private final String MAP_BIN = "mapBin";
	private final int MAP_SPLIT_SIZE = 100;
	
	@Before
	public void setUp() throws Exception {
		 client = new AerospikeClient(HOST, 3000);
		 MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
		 
		 // Use the underlying functions for restricted functions
		 library = new AdaptiveMap(client, NAMESPACE, SET, MAP_BIN, policy, false, MAP_SPLIT_SIZE);
		 
		 // Set up the interface to use for primary operations
		 adaptiveMapWithValueKey = library;
		 
		 adaptiveMapWithDigestKey = new AdaptiveMap(client, NAMESPACE, SET, MAP_BIN, policy, true, MAP_SPLIT_SIZE);
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}
	
	@Test
	public void testDepthLevel() {
		assertEquals(0, library.depthLevel(0));
		assertEquals(1, library.depthLevel(1));
		assertEquals(1, library.depthLevel(2));
		assertEquals(2, library.depthLevel(3));
		assertEquals(2, library.depthLevel(4));
		assertEquals(2, library.depthLevel(5));
		assertEquals(2, library.depthLevel(6));
		assertEquals(3, library.depthLevel(7));
		assertEquals(3, library.depthLevel(14));
		assertEquals(4, library.depthLevel(15));
	}

	@Test
	public void testComputeBlockNumber() {
		byte[] digest = new byte[] { (byte)0b10101010, 0b00000000 };
		assertEquals(0, library.computeBlockNumber(digest, new byte[] {0b00000000}));
		assertEquals(1, library.computeBlockNumber(digest, new byte[] {0b00000001}));
		assertEquals(4, library.computeBlockNumber(digest, new byte[] {0b00000011}));
		assertEquals(9, library.computeBlockNumber(digest, new byte[] {0b00010011}));
	}
	
	public void testInsert(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final Key recordKey = new Key(NAMESPACE, SET, recordKeyStr);
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.delete(null, recordKey);
		
		map.put(recordKeyStr, mapKey, null, Value.get("test"));
		
		assertTrue(client.exists(null, recordKey));
		Object result = map.get(recordKeyStr, mapKey);
		assertEquals("test", result);		
	}

	@Test
	public void testInsertWithValueKey() {
		testInsert(adaptiveMapWithValueKey);
	}

	@Test
	public void testInsertWitDigestKey() {
		testInsert(adaptiveMapWithDigestKey);
	}

	public void testSplitMap(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 1.5);
		for (int i = 0; i <  count; i++) {
			map.put(recordKeyStr, mapKey+i, null, Value.get(mapKey+i));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		
		now = System.nanoTime();
		for (int i = 0; i < count; i++) {
			assertEquals(mapKey+i, map.get(recordKeyStr, mapKey+i));
		}
		time = System.nanoTime() - now;
		System.out.printf("Read %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
	}

	@Test
	public void testSplitMapValueKey() {
		System.out.printf("\n*** testSplitMapValueKey ***\n");
		testSplitMap(adaptiveMapWithValueKey);
	}

	@Test
	public void testSplitMapDigestKey() {
		System.out.printf("\n*** testSplitMapDigestKey ***\n");
		testSplitMap(adaptiveMapWithDigestKey);
	}

	public void testMultipleSplitMap(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 10);
		for (int i = 0; i <  count; i++) {
			String mapKeyToUse = mapKey + i;
			map.put(recordKeyStr, mapKeyToUse, null, Value.get(mapKeyToUse));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		
		now = System.nanoTime();
		for (int i = 0; i < count; i++) {
			String mapKeyToUse = mapKey + i;
			assertEquals(mapKeyToUse, map.get(recordKeyStr, mapKeyToUse));
		}
		time = System.nanoTime() - now;
		System.out.printf("Read %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
	}
	
	@Test
	public void testMultipleSplitMapValueKey() {
		System.out.printf("\n*** testMultipleSplitMapValueKey ***\n");
		testMultipleSplitMap(adaptiveMapWithValueKey);
	}

	@Test
	public void testMultipleSplitMapDigestKey() {
		System.out.printf("\n*** testMultipleSplitMapDigestKey ***\n");
		testMultipleSplitMap(adaptiveMapWithDigestKey);
	}

	public void testMultipleSplitAndRead(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 3);
		for (int i = 0; i < count; i++) {
			String mapKeyToUse = mapKey + i;
			List<Object> values = Arrays.asList(new Object[] { i*1000, mapKeyToUse });
			map.put(recordKeyStr, mapKeyToUse, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		
		// Pick a point 1/2 way through the map and retrieve the values > this value
		long midpointStart = (count/2)*1000 - 500;
		System.out.println("Returning all numbers > " + midpointStart);
		Operation operation = MapOperation.getByValueRange(MAP_BIN, Value.get(Arrays.asList(new Object[] {midpointStart, "" })), null, MapReturnType.VALUE);
		
		now = System.nanoTime();
		Set<Record> results = map.getAll(null, recordKeyStr, operation);
		time = System.nanoTime() - now;

		// Check the reads using raw reads
		Set<Long> expectedNumbers = new HashSet<>();
		int resultCount = 0;
		for (long i = (count/2)*1000; i < count * 1000; i += 1000, resultCount++) {
			// Round the number up to the correct 1000
			expectedNumbers.add(i);
		}
		
		for (Record result : results) {
			// Each value in the map is itself a list of (long, String)
			List<List<Object>> list = (List<List<Object>>) result.getList(MAP_BIN);
			for (List<Object> thisValue : list) {
				long longVal = (long) thisValue.get(0);
				String stringVal = (String) thisValue.get(1);
				
				assertTrue("Number " +longVal+" was not found in the expectd results", expectedNumbers.remove(longVal));
			}
		}
		assertEquals("Not all expected numbers were found. Missing ones were: " + expectedNumbers, 0, expectedNumbers.size());
		
		System.out.printf("Read %d records with %d results in %.1fms\n", results.size(), resultCount, (time/1000000.0));
	}
	
	@Test
	public void testMultipleSplitAndReadAllValueKey() {
		System.out.printf("\n*** testMultipleSplitAndReadAllValueKey ***\n");
		testMultipleSplitAndRead(adaptiveMapWithValueKey);
	}

	@Test
	public void testMultipleSplitAndReadAllDigestKey() {
		System.out.printf("\n*** testMultipleSplitAndReadAllDigestKey ***\n");
		testMultipleSplitAndRead(adaptiveMapWithDigestKey);
	}

	private String numToDigits(int num, int digitCount) {
		String val = "00000000000000" + num;
		return val.substring(val.length() - digitCount);
	}
	public void testMultipleSplitAndReadAll(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 3);
		for (int i = 0; i < count; i++) {
			String mapKeyToUse = mapKey + numToDigits(i, 5);
			List<Object> values = Arrays.asList(new Object[] { i*1000, mapKeyToUse });
			map.put(recordKeyStr, mapKeyToUse, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		
		now = System.nanoTime();
		TreeMap<Object, Object> results = (TreeMap<Object, Object>) map.getAll(null, recordKeyStr);
		time = System.nanoTime() - now;

		NavigableSet<Object> navSet = results.navigableKeySet();
		int i = 0;
		for (Object key : navSet) {
			List<Object> value = (List<Object>) results.get(key);
			String mapKeyToUse = mapKey + numToDigits(i, 5);
			assertEquals("Expected key " + i + " to be '" + mapKeyToUse +"' but received '" + key + "'", key, mapKeyToUse);
			assertEquals("Expected value " + i + " to be '" + (i*1000) +"' but received '" + value.get(0) + "'", value.get(0), (long)i*1000);
			i++;
		}
		assertEquals("Not enough records returned", count, i);
		System.out.printf("Read %d results in %.1fms\n", results.size(), (time/1000000.0));
	}
	
	@Test
	public void testMultipleSplitAndReadValueKey() {
		System.out.printf("\n*** testMultipleSplitAndReadValueKey ***\n");
		testMultipleSplitAndReadAll(adaptiveMapWithValueKey);
	}

	private void dumpMap(Map<Object, Object> data) {
		for (Object key : data.keySet()) {
			System.out.printf("%s -> %s\n", (key instanceof ByteBuffer) ? "ByteBuffer(" + Buffer.bytesToHexString(((ByteBuffer)key).array()) + ")" : Buffer.bytesToHexString((byte [])key)
					, data.get(key));
		}
	}
	
	@Test
	public void testDelete() {
		System.out.printf("\n*** testDelete ***\n");
		IAdaptiveMap map = adaptiveMapWithValueKey;
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 3);
		for (int i = 0; i < count; i++) {
			String mapKeyToUse = mapKey + i;
			List<Object> values = Arrays.asList(new Object[] { i*1000, mapKeyToUse });
			map.put(recordKeyStr, mapKeyToUse, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		String mapKeyToRemove = mapKey + "1";
		Object result = map.get(recordKeyStr, mapKeyToRemove);
		System.out.println("Result of get record before delete = " + result);
		assertNotNull(result);
		now = System.nanoTime();
		Object deletedRecord = map.delete(null, recordKeyStr, mapKey+"1");
		time = System.nanoTime() - now;
		System.out.printf("map.delete returned: %s in %.1fms\n", deletedRecord, time/1000000.0);
		assertNotNull(deletedRecord);
		
		result = map.get(recordKeyStr, mapKeyToRemove);
		System.out.println("Result after delete = " + result);
		assertNull(result);
	}
	
	@Test
	public void testMultiRecordGet() {
		System.out.printf("\n*** testMultiRecordGet ***\n");
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);
		final int DAYS = 30;
		final int COUNT_PER_DAY = 10*MAP_SPLIT_SIZE;
		String basePart = "base";
		int count = 0;
		for (int day = 1; day <= DAYS; day++) {
			for (int i = 0; i < COUNT_PER_DAY+5*day; i++) {
				Map<String, Integer> map = new HashMap<>();
				map.put("day", day);
				map.put("i", i);
				map.put("count", count++);
				this.adaptiveMapWithValueKey.put(basePart + ":" + day, day + "-" + i, null, Value.get(map));
				
			}
		}
		String[] keys = new String[DAYS];
		for (int i = 1; i <= DAYS; i++) {
			keys[i-1] = basePart + ":" + i;
		}
		
		long now = System.nanoTime();
		TreeMap<Object, Object>[] records = this.adaptiveMapWithValueKey.getAll(null, keys);
		long time = System.nanoTime() - now;
		
		int totalCount = 0;
		Set<Long> counts = new HashSet<>();
		for (int day = 1; day <= DAYS; day++) {
			TreeMap<Object, Object> dayRecords = records[day-1];
			Set<Long> dayCounts = new HashSet<>();
			for (Object valueObj : dayRecords.values()) {
				Map<String, Long> value = (Map<String, Long>) valueObj;
				dayCounts.add(value.get("i"));
				counts.add(value.get("count"));
				totalCount++;
			}
			// Now validate that we have all the day counters
			for (long j = 0; j < COUNT_PER_DAY+5*day; j++) {
				assertTrue("Map for day "+day+" does not contain day-count of " + j, dayCounts.contains(j));
				dayCounts.remove(j);
			}
			assertTrue("Day counts for day " + day + " should be empty, but still contains " + dayCounts, dayCounts.isEmpty());
		}
		long aCount = 0;
		for (int day = 1; day <= DAYS; day++) {
			for (int i = 0; i < COUNT_PER_DAY+5*day; i++) {
				assertTrue("Missing count of " + aCount, counts.contains(aCount));
				counts.remove(aCount);
				aCount++;
			}
		}
		assertTrue("Counts should be empty, but still contains " + counts, counts.isEmpty());
		System.out.printf("Retreived %d days records with %d total entries in %.1fms\n", DAYS, totalCount, time / 1000000.0);
	}
	/*
	@Test
	public void testCdt() {
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		final Key key = new Key("test", "testAdapt", recordKeyStr);
		final MapPolicy mp = new MapPolicy(MapOrder.UNORDERED, 0);
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		for (long i = 1; i < 10; i++) {
			String mapKeyToUse = mapKey + i;
			List<Object> values = Arrays.asList(new Object[] { i*1000, mapKeyToUse });

			client.operate(null, key, MapOperation.put(mp, MAP_BIN, Value.get(Crypto.computeDigest("", Value.get(mapKeyToUse))), Value.get(values)));
		}
		
		// Now get the map, iterate through the values, put them into a new map, re-insert them
		Record r = client.get(null, key, MAP_BIN);
		Map<Object, Object> data = (Map<Object, Object>) r.getMap(MAP_BIN);
		dumpMap(data);
		
		r = client.operate(null, key, Operation.get(MAP_BIN));
		data = (Map<Object, Object>) r.getMap(MAP_BIN);
		dumpMap(data);
		
		Map<Object, Object> newMap = new HashMap<>();
		for (Object k : data.keySet()) {
			newMap.put(k, data.get(k));
		}
		client.put(null, key, new Bin(MAP_BIN, Value.get(newMap)));
		
		client.operate(null, key, MapOperation.put(mp, MAP_BIN, Value.get(Crypto.computeDigest("", Value.get(mapKey + 7))), Value.get(Arrays.asList(new Object[] {7000, "Second 7000" }))));
		
		System.out.println(client.operate(null, key, MapOperation.getByValueRange(MAP_BIN, Value.get(Arrays.asList(new Object[] {6500, "" })), null, MapReturnType.VALUE)));
	}
	*/
}
