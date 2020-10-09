package com.aerospike.storage.adaptive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.task.RegisterTask;

public class TestAdaptiveUdfInvocation {
	private final String HOST = "127.0.0.1";
	private AerospikeClient client;
	private final String NAMESPACE = "test";
	private final String SET = "testAdapt";
	private final String MAP_BIN = "mapBin";
	private final int MAP_SPLIT_SIZE = 100;

	@Test
	public void callUdfBeforeSplit() {
		 client = new AerospikeClient(HOST, 3000);
		 MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
		 
		 // Use the underlying functions for restricted functions
		 AdaptiveMap library = new AdaptiveMap(client, NAMESPACE, SET, MAP_BIN, policy, false, MAP_SPLIT_SIZE);

		 client.truncate(null, NAMESPACE, SET, null);
		 final String recordKey = "recordKey";
		 
		 for (int i = 0; i <MAP_SPLIT_SIZE-1; i++) {
			 List<Object> value = new ArrayList<>();
			 value.add("value"+i);
			 value.add("secondValue" + i + "-" + i);
			 library.put(null, recordKey, "mapKey" + i, null, Value.get(value));
		 }
		 
		 String udfText =
		 		"function removeData(rec, mapKey, dataBin, expectedData, position)\n" +
		 		"debug(\"MapKey = %s\", mapKey)\n" + 
		 		"debug(\"expectedData = %s\", expectedData)\n" + 
		 		"local changed = false\n" + 
		 		"local aMap = rec[dataBin]\n" + 
		 		"local listValue = aMap[mapKey]\n" +
		 		"debug(\"%s\", listValue)\n" +
		 		"if position > 0 and position <= list.size(listValue) and listValue[position] == expectedData then\n" +
		 			"debug(\"Found element!\")\n" +
		 			"map.remove(aMap, mapKey)\n"+
		 			"rec[dataBin] = map\n"+
		 			"aerospike:update(rec)\n"+
		 			"return 1\n" +
		 		"end\n" + 
		 		"return 0\n"+
		 		"end";
		 RegisterTask task = client.registerUdfString(null, udfText, "testCode.lua", Language.LUA);
		 task.waitTillComplete();

		 Object record = library.get(recordKey, "mapKey5");
		 assertNotNull(record);
		 assertTrue(record instanceof List);

		 Object result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("secondValue5-5"), Value.get(2));
		 System.out.println(result);
		 
		 record = library.get(recordKey, "mapKey5");
		 assertNull(record);
		 
		 // Re-insert the record 
		 List<Object> value = new ArrayList<>();
		 value.add("value5");
		 value.add("secondValue5-5");
		 library.put(null, recordKey, "mapKey5", null, Value.get(value));
		 
		 // Conditionally delete with a different second value, which should fail
		 result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("secondValue5-4"), Value.get(2));
		 System.out.println(result);
		 
		 record = library.get(recordKey, "mapKey5");
		 assertNotNull(record);
		 assertTrue(record instanceof List);		 

		 // Test some edge conditions
		 result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("secondValue5-5"), Value.get(-1));
		 assertEquals(0L, result);
		 result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("secondValue5-5"), Value.get(100000));
		 assertEquals(0L, result);
		 result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("bob"), Value.get(2));
		 assertEquals(0L, result);

		 client.close();
	}
	
	@Test
	public void callUdfAfterSplit() {
		 client = new AerospikeClient(HOST, 3000);
		 MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
		 
		 // Use the underlying functions for restricted functions
		 AdaptiveMap library = new AdaptiveMap(client, NAMESPACE, SET, MAP_BIN, policy, false, MAP_SPLIT_SIZE);

		 client.truncate(null, NAMESPACE, SET, null);
		 final String recordKey = "recordKey";
		 
		 for (int i = 0; i <MAP_SPLIT_SIZE*10; i++) {
			 List<Object> value = new ArrayList<>();
			 value.add("value"+i);
			 value.add("secondValue" + i + "-" + i);
			 library.put(null, recordKey, "mapKey" + i, null, Value.get(value));
		 }
		 
		 String udfText =
		 		"function removeData(rec, mapKey, dataBin, expectedData, position)\n" +
		 		"debug(\"MapKey = %s\", mapKey)\n" + 
		 		"debug(\"expectedData = %s\", expectedData)\n" + 
		 		"local changed = false\n" + 
		 		"local aMap = rec[dataBin]\n" + 
		 		"local listValue = aMap[mapKey]\n" +
		 		"debug(\"%s\", listValue)\n" +
		 		"if position > 0 and position <= list.size(listValue) and listValue[position] == expectedData then\n" +
		 			"debug(\"Found element!\")\n" +
		 			"map.remove(aMap, mapKey)\n"+
		 			"rec[dataBin] = map\n"+
		 			"aerospike:update(rec)\n"+
		 			"return 1\n" +
		 		"end\n" + 
		 		"return 0\n"+
		 		"end";
		 RegisterTask task = client.registerUdfString(null, udfText, "testCode.lua", Language.LUA);
		 task.waitTillComplete();

		 Object record = library.get(recordKey, "mapKey5");
		 assertNotNull(record);
		 assertTrue(record instanceof List);

		 Object result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("secondValue5-5"), Value.get(2));
		 System.out.println(result);
		 
		 record = library.get(recordKey, "mapKey5");
		 assertNull(record);
		 
		 // Re-insert the record 
		 List<Object> value = new ArrayList<>();
		 value.add("value5");
		 value.add("secondValue5-5");
		 library.put(null, recordKey, "mapKey5", null, Value.get(value));
		 
		 // Conditionally delete with a different second value, which should fail
		 result = library.executeUdfOnRecord(null, recordKey, "mapKey5", null, "testCode", "removeData", Value.get("secondValue5-4"), Value.get(2));
		 System.out.println(result);
		 
		 record = library.get(recordKey, "mapKey5");
		 assertNotNull(record);
		 assertTrue(record instanceof List);
		 client.close();
	}

}
