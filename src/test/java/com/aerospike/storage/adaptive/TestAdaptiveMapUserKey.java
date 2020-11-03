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
import com.aerospike.client.policy.WritePolicy;


public class TestAdaptiveMapUserKey {

	private final String HOST = "127.0.0.1";
	private AerospikeClient client;
	private IAdaptiveMap adaptiveMapWithValueKey;
	private final String NAMESPACE = "test";
	private final String SET = "testAdapt";
	private final String MAP_BIN = "mapBin";
	private final int MAP_SPLIT_SIZE = 100;

	@Before
	public void setUp() throws Exception {
		 String host = getEnvString("AS_HOST", HOST);
		 client = new AerospikeClient(host, 3000);
		 MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);

		 // Use the underlying functions for restricted functions
		 adaptiveMapWithValueKey = new AdaptiveMapUserSuppliedKey(client, NAMESPACE, SET, MAP_BIN, policy, MAP_SPLIT_SIZE);
		 client.truncate(null, NAMESPACE, SET, null);
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}

	public String getEnvString(String var, String dflt) {
		String val = (System.getenv(var) == null) ? dflt : System.getenv(var);
		System.out.printf("%s:%s # export %s=<str> to modifiy value\n",var,val,var);
		return val;
	}

	public void testInsert(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final Key recordKey = new Key(NAMESPACE, SET, recordKeyStr);
		final long mapKey = 12;
		// Clean up after previous runs
		WritePolicy wp = new WritePolicy();
		wp.durableDelete = true;

		client.delete(wp, recordKey);

		map.put(null, recordKeyStr, mapKey, null, Value.get("test"));

		assertTrue(client.exists(null, recordKey));
		Object result = map.get(recordKeyStr, mapKey);
		assertEquals("test", result);
	}

	@Test
	public void testInsertWithValueKey() {
		testInsert(adaptiveMapWithValueKey);
	}

	public void testSplitMap(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 1.5);
		for (long i = 0; i < count; i++) {
			map.put(null, recordKeyStr, i, null, Value.get(i));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);

		now = System.nanoTime();
		for (long i = 0; i < count; i++) {
			assertEquals(i, map.get(recordKeyStr, i));
		}
		time = System.nanoTime() - now;
		System.out.printf("Read %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
	}

	@Test
	public void testSplitMapValueKey() {
		System.out.printf("\n*** testSplitMapValueKey ***\n");
		testSplitMap(adaptiveMapWithValueKey);
	}

	public void testMultipleSplitMap(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = MAP_SPLIT_SIZE * 10;
		for (int i = 0; i <  count; i++) {
			map.put(null, recordKeyStr, i, null, Value.get("key-" +i));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);

		now = System.nanoTime();
		for (long i = 0; i < count; i++) {
			assertEquals("key-" + i, map.get(recordKeyStr, i));
		}
		time = System.nanoTime() - now;
		System.out.printf("Read %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
	}

	@Test
	public void testMultipleSplitMapValueKey() {
		System.out.printf("\n*** testMultipleSplitMapValueKey ***\n");
		testMultipleSplitMap(adaptiveMapWithValueKey);
	}

	public void testMultipleSplitAndRead(IAdaptiveMap map) {
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = MAP_SPLIT_SIZE * 3;
		for (int i = 0; i < count; i++) {
			List<Object> values = Arrays.asList(new Object[] { i*1000, "key-" + i });
			map.put(null, recordKeyStr, i, null, Value.get(values));
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
		long count = MAP_SPLIT_SIZE * 3;
		for (int i = 0; i < count; i++) {
			List<Object> values = Arrays.asList(new Object[] { i*1000, "Key-" + i });
			map.put(null, recordKeyStr, i, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);

		now = System.nanoTime();
		List<String> results = map.getAll(null, recordKeyStr, (mappKey, data, blockNum) -> {
			return ""+mappKey+","+data;
		});
		time = System.nanoTime() - now;

		int i = 0;
		for (String result : results) {
			String expected = "" + i + ",["+(i*1000)+", Key-" + i +"]";
			assertEquals("Expected combo to be " + expected + " to be '" + i +"' but received '" + result + "'", expected, result);
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

	@Test
	public void testDelete() {
		System.out.printf("\n*** testDelete ***\n");
		IAdaptiveMap map = adaptiveMapWithValueKey;
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = MAP_SPLIT_SIZE * 3;
		for (int i = 0; i < count; i++) {
			List<Object> values = Arrays.asList(new Object[] { i*1000, "key-"+i });
			map.put(null, recordKeyStr, i, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		long mapKeyToRemove = 1;
		Object result = map.get(recordKeyStr, mapKeyToRemove);
		System.out.println("Result of get record before delete = " + result);
		assertNotNull(result);
		now = System.nanoTime();
		Object deletedRecord = map.delete(null, recordKeyStr, 1L);
		time = System.nanoTime() - now;
		System.out.printf("map.delete returned: %s in %.1fms\n", deletedRecord, time/1000000.0);
		assertNotNull(deletedRecord);

		result = map.get(recordKeyStr, mapKeyToRemove);
		System.out.println("Result after delete = " + result);
		assertNull(result);
	}

	@Test
	public void testCountNoSplit() {
		System.out.printf("\n*** testCountNoSplit ***\n");
		IAdaptiveMap map = adaptiveMapWithValueKey;
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE / 2);
		for (int i = 0; i < count; i++) {
			List<Object> values = Arrays.asList(new Object[] { i*1000, i});
			map.put(null, recordKeyStr, i, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		now = System.nanoTime();
		int mapCount = map.countAll(null, recordKeyStr);
		time = System.nanoTime() - now;
		System.out.printf("map.count returned: %s in %.1fms\n", mapCount, time/1000000.0);
		assertEquals(mapCount, count);
	}
	
	@Test
	public void testCountSplit() {
		System.out.printf("\n*** testCountSplit ***\n");
		IAdaptiveMap map = adaptiveMapWithValueKey;
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		long now = System.nanoTime();
		long count = (long)(MAP_SPLIT_SIZE * 3);
		for (int i = 0; i < count; i++) {
			List<Object> values = Arrays.asList(new Object[] { i*1000, i });
			map.put(null, recordKeyStr, i, null, Value.get(values));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", count, (time/1000000.0), (time/1000000.0)/count);
		int mapCount = map.countAll(null, recordKeyStr);
		time = System.nanoTime() - now;
		System.out.printf("map.count returned: %s in %.1fms\n", mapCount, time/1000000.0);
		assertEquals(count, mapCount);
	}
	
	@Test
	public void testSplitOnRecordTooBig() {
		System.out.printf("\n*** testSplitOnRecordTooBig ***\n");
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		final int COUNTER = 5_000;
		IAdaptiveMap map = new AdaptiveMapUserSuppliedKey(client, NAMESPACE, SET, MAP_BIN, new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT), COUNTER);
		final String recordKeyStr = "key1";
		final String mapKey = "mapKey";
		String data = "1234567890abcdef";
		for (int i = 0; i <= 6; i++) {
			data = data + data;
		}

		long now = System.nanoTime();
		for (int i = 0; i < COUNTER; i++) {
			map.put(null, recordKeyStr, i, null, Value.get(data));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", COUNTER, (time/1000000.0), (time/1000000.0)/COUNTER);
		now = System.nanoTime();
		int mapCount = map.countAll(null, recordKeyStr);
		time = System.nanoTime() - now;
		System.out.printf("map.count returned: %s in %.1fms\n", mapCount, time/1000000.0);
		assertEquals(COUNTER, mapCount);
	}

	@Test
	public void testSplitFirstSubblock() {
		System.out.printf("\n*** testSplitFirstSubblock ***\n");
		// Clean up after previous runs
		client.truncate(null, NAMESPACE, SET, null);

		final int COUNTER = 10;
		IAdaptiveMap map = new AdaptiveMapUserSuppliedKey(client, NAMESPACE, SET, MAP_BIN, new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT), COUNTER);
		final String recordKeyStr = "key1";
		String data = "1234567890abcdef";

		long now = System.nanoTime();
		for (int i = 100; i > 0; i--) {
			map.put(null, recordKeyStr, i, null, Value.get(data));
		}
		long time = System.nanoTime() - now;
		System.out.printf("Inserted %d records in %.1fms (%.1fms avg)\n", COUNTER, (time/1000000.0), (time/1000000.0)/COUNTER);
		now = System.nanoTime();
		int mapCount = map.countAll(null, recordKeyStr);
		time = System.nanoTime() - now;
		System.out.printf("map.count returned: %s in %.1fms\n", mapCount, time/1000000.0);
		assertEquals(100, mapCount);
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
				this.adaptiveMapWithValueKey.put(null, basePart + ":" + day, i, null, Value.get(map));

			}
		}
		String[] keys = new String[DAYS];
		for (int i = 1; i <= DAYS; i++) {
			keys[i-1] = basePart + ":" + i;
		}

		long now = System.nanoTime();
		List<Map<String, Long>>[] records = this.adaptiveMapWithValueKey.getAll(null, keys, (mapKey, value, block) -> {
			return (Map<String, Long>)value;
		});
		long time = System.nanoTime() - now;

		int totalCount = 0;
		Set<Long> counts = new HashSet<>();
		for (int day = 1; day <= DAYS; day++) {
			List<Map<String, Long>> dayRecords = records[day-1];
			Set<Long> dayCounts = new HashSet<>();
			for (Map<String, Long> value : dayRecords) {
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
