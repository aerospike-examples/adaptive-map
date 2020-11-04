package com.aerospike.storage.adaptive.utils;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.storage.adaptive.AdaptiveMap;
import com.aerospike.storage.adaptive.AdaptiveMapUserSuppliedKey;
import com.aerospike.storage.adaptive.BitwiseData;
import com.aerospike.storage.adaptive.IAdaptiveMap;
import com.aerospike.storage.adaptive.utils.PerformanceTest.MapType;

/**
 * Count the number of records
 * <pre>java -jar adaptive-map-1.0-full.jar count -n test -s testSet -b map -k key1 -e</pre>
 * 
 * The "explain" option (-e) will print a hierachy of the splits of the blocks:
 * <pre>
 * key key1 has 100 members
                                                                              0  
                                       ---------------------------------------------------------------------------------
                                      1                                                                               2  
                   -----------------------------------------                                       -----------------------------------------
                  3                                       4                                       5                                       6  
         ---------------------                   ---------------------                   ---------------------                   ---------------------
       -7-                 -8-                  9                   10                 -11-                 12                  13                  14 
        9                   8                                                           10                                                             
                                            -----------         -----------                             -----------         -----------         -----------
                                          -19-      -20-      -21-      -22-                           25       -26-      -27-      -28-      -29-      -30-
                                           9         5         7         10                                      7         6         5         4         9  
                                                                                                      ------                                                  
                                                                                                    -51- -52-                                                  
                                                                                                     6    5                                                    

 * </pre>
 * @author timfaulkes
 *
 */
public class CountCommand extends Command {

	private String binName;
	private String key;
	private boolean explain;
	private boolean useUdf;
	private boolean shouldRegisterUdf;
	private MapType type = MapType.NORMAL;

	private boolean isNonLeafNode(BitwiseData data, int blockNum) {
		return data.get(blockNum);
	}
	
	private boolean isLeafNode(BitwiseData data, int blockNum) {
		int parentBlock = (blockNum-1)/2;
		return (data.get(blockNum) == false && (blockNum == 0 || data.get(parentBlock) == true));
	}
	
	private int maxSplits(BitwiseData data, int level, int count) {
		if (data.get(level)) {
			int left = maxSplits(data, 2*level+1, count+1);
			int right = maxSplits(data, 2*level+2, count+1);
			return Math.max(left, right);
		}
		return count;
	}
	
	private int highestBlock(BitwiseData data, int level) {
		if (data.get(level)) {
			int left = highestBlock(data, 2*level+1);
			int right = highestBlock(data, 2*level+2);
			return Math.max(left, right);
		}
		return level;
	}
	
	private String getChars(String thisChar, int count) {
		String chars = "";
		for (int i = 0; i < count; i++) {
			chars += thisChar;
		}
		return chars;
	}

	private String getSpaces(int count) {
		return getChars(" ", count);
	}
	
	private String padToSize(int number, int size) {
		return padToSize(Integer.toString(number), size);
	}
	
	private String padToSize(String string, int size) {
		int excess = size - string.length();
		if (excess > 0) {
			return getSpaces(excess / 2) + string + getSpaces(excess - (excess/2));
		}
		else {
			return string;
		}
	}
	
	private int getMiddleOfBottomBlock(int index, int startBottomBlock, int longestBlockLen) {
		int offset = index - startBottomBlock;
		return (longestBlockLen + 1) * offset + longestBlockLen/2;
	}
	
	private int getBlockMiddleOffset(int index, int startBottomBlock, int longestBlockLen) {
		if (index >= startBottomBlock) {
			return getMiddleOfBottomBlock(index, startBottomBlock, longestBlockLen);
		}
		else {
			int left = getBlockMiddleOffset(2*index+1, startBottomBlock, longestBlockLen);
			int right = getBlockMiddleOffset(2*index+2, startBottomBlock, longestBlockLen);
			return (left+right)/2;
		}
	}
	
	private int getStartIndexOfBottomBlock(int maxSplits) {
		return (1<<maxSplits)-1;
	}
	
	private void drawCountsForLevel(Key key, BitwiseData bitwiseData, int level, int maxSplits, int longestBlockLen) {
		int blocksThisLevel = 1 << level;
		int startBlockBottomLevel = getStartIndexOfBottomBlock(maxSplits);
		int startBlock = blocksThisLevel - 1;
		
		StringBuffer sb = new StringBuffer();
		
		for (int i = 0; i < blocksThisLevel; i++) {
			int currentBlock = startBlock + i;
			int middleOffset = getBlockMiddleOffset(currentBlock, startBlockBottomLevel, longestBlockLen);
			int startOffset = middleOffset - longestBlockLen/2;

			String id = key.userKey.toString() + (currentBlock == 0 ? "" : (":"+currentBlock));
			// Insert spaces first
			sb.append(getSpaces(startOffset - sb.length()));
			if (isLeafNode(bitwiseData, currentBlock)) {
				Record record = getAerospikeClient().operate(null, new Key(key.namespace, key.setName, id), MapOperation.getByIndexRange(binName, 0, MapReturnType.COUNT));
				String value;
				if (record == null || !record.bins.containsKey(binName)) {
					value = "???";
				}
				else {
					value = Integer.toString(record.getInt(binName));
				}
				sb.append(padToSize(value, longestBlockLen));
			}
			else {
				sb.append(getSpaces(longestBlockLen));
			}
		}
		System.out.println(sb.toString());
	}
	
	private void drawSplitsForLevel(Key key, BitwiseData bitwiseData, int level, int maxSplits, int longestBlockLen) {
		int blocksThisLevel = 1 << level;
		int startBlockBottomLevel = getStartIndexOfBottomBlock(maxSplits);
		int startBlock = blocksThisLevel - 1;
		StringBuffer sb = new StringBuffer();

		// -------------------------------------------------------
		// First draw the links between our parent and our sibling
		// -------------------------------------------------------
		if (level > 0) {
			for (int i = 0; i < blocksThisLevel; i+=2) {
				int currentBlock = startBlock + i;
				int middleOffsetFirst = getBlockMiddleOffset(currentBlock, startBlockBottomLevel, longestBlockLen);
				int middleOffsetEnd = getBlockMiddleOffset(currentBlock+1, startBlockBottomLevel, longestBlockLen);
				
				// Insert spaces first
				sb.append(getSpaces(middleOffsetFirst - sb.length()));
				if (isLeafNode(bitwiseData, currentBlock) || isNonLeafNode(bitwiseData, currentBlock)) {
					sb.append(getChars("-", middleOffsetEnd - middleOffsetFirst + 1));
				}
				else {
					sb.append(getSpaces(middleOffsetEnd - middleOffsetFirst + 1));
				}
			}
			System.out.println(sb.toString());
		}

		// -------------------------------------------------------
		// Then draw the block number
		// -------------------------------------------------------
		sb = new StringBuffer();
		boolean needToDrawLeafCounts = false;
		for (int i = 0; i < blocksThisLevel; i++) {
			int currentBlock = startBlock + i;
			int middleOffset = getBlockMiddleOffset(currentBlock, startBlockBottomLevel, longestBlockLen);
			int startOffset = middleOffset - longestBlockLen/2;
			
			// Insert spaces first
			sb.append(getSpaces(startOffset - sb.length()));
			if (isLeafNode(bitwiseData, currentBlock)) {
				sb.append(padToSize("-"+currentBlock+"-", longestBlockLen));
				needToDrawLeafCounts = true;
			}
			else if (isNonLeafNode(bitwiseData, currentBlock)) {
				sb.append(padToSize(currentBlock, longestBlockLen));
			}
			else {
				sb.append(getSpaces(longestBlockLen));
			}
		}
		System.out.println(sb.toString());
		if (needToDrawLeafCounts) {
			drawCountsForLevel(key, bitwiseData, level, maxSplits, longestBlockLen);
		}
	}
	
	/**
	 * Draw the splits for a normal adaptive map
	 * @param client
	 * @param key
	 * @param mapBin
	 */
	private void drawSplits(IAerospikeClient client, Key key, String mapBin) {
		Record record = client.get(null, key);
		if (record != null) {
			byte[] blocks = (byte[])record.getValue("blks");
			BitwiseData bitwiseData = new BitwiseData(blocks);
			int maxSplits = maxSplits(bitwiseData, 0, 0);
			int highestBlock = highestBlock(bitwiseData, 0);
			int longestBlockLen = Integer.toString(highestBlock).length()+2;
			for (int i = 0; i <= maxSplits; i++) {
				drawSplitsForLevel(key, bitwiseData, i, maxSplits, longestBlockLen);
			}
		}
	}

	/**
	 * Draw the counts for a time-sorted adaptive map
	 * @param client
	 * @param key
	 * @param mapBin
	 */
	private void drawCounts(IAerospikeClient client, Key key, String mapBin) {
		Record record = client.get(null, key);
		if (record != null) {
			@SuppressWarnings("unchecked")
			Map<Long, Long> data = (Map<Long, Long>) record.getMap("blks");
			if (data == null) {
				System.out.printf("root: %d\n", record.getMap(binName).size());
			}
			for (long subMapStartKey : data.keySet()) {
				long subMapRecordKey = data.get(subMapStartKey);
				record = getAerospikeClient().operate(null, new Key(key.namespace, key.setName, key.userKey + ":" + subMapRecordKey), MapOperation.getByIndexRange(binName, 0, MapReturnType.COUNT));
				if (record != null) {
					System.out.printf("%d -> %d: %d\n",	subMapStartKey, subMapRecordKey, record.getLong(binName));
				}
				else {
					System.out.printf("%d -> %d: null\n",	subMapStartKey, subMapRecordKey);
				}
			}
		}
	}

	private long countViaUdf(IAerospikeClient client, String namespace, String setName, String binName, boolean registerUdf) {
        File udfFile = new File("src/main/resources/count.lua");
        if (registerUdf) {
    		LuaConfig.SourceDirectory = udfFile.getParent();
            RegisterTask rt = client.register(null, udfFile.getPath(), udfFile.getName(), Language.LUA);
            rt.waitTillComplete();
        }
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(setName);
		stmt.setAggregateFunction("count", "countMapItems", Value.get(binName));

		// Set timeout policies to make sure we don't timeout when counting large data sets
		QueryPolicy qp = new QueryPolicy();
		qp.maxConcurrentNodes = 0;
		qp.socketTimeout = 3_600_000;
		ResultSet rs = client.queryAggregate(qp, stmt);
		rs.next();
		return (Long)rs.getObject();
	}

	@Override
	protected void addSubCommandOptions(Options options) {
		options.addRequiredOption("b", "bin", true, "Specifies the bin which contains the map data. (REQUIRED)");
		options.addOption("k", "key", true, "Count sub-items associated with a specific record. If not provided, all records will be counted.");
		options.addOption("e", "explain", false, "Explain the splits and counts for a single key record. Ignored if --key is not specified.");
		options.addOption("u", "udf", false, "Use a UDF to count the records instead of a client-side scan. Ignored if --key is specified.");
		options.addOption("r", "register", false, "Register the counting UDF. If not specified, assumes the UDF has already been registered. Ignored if --udf is not specified.");
		options.addOption("T", "type", true, "Specify the map type (TimeSorted or Normal). Default: Normal");
	}
	
	@Override
	protected void extractSubCommandOptions(CommandLine commandLine) {
		this.binName = commandLine.getOptionValue("bin");
		this.key = commandLine.getOptionValue("key");
		this.explain = commandLine.hasOption("explain");
		this.useUdf = commandLine.hasOption("udf");
		this.shouldRegisterUdf = commandLine.hasOption("register");
		this.type = MapType.getMapType(commandLine.getOptionValue("type"));
	}
	
	@Override
	protected void run(CommandType commandType, String[] argments) {
		super.parseCommandLine(commandType.getName(), argments);
		IAerospikeClient client = super.connect();
		
		IAdaptiveMap map;
		MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, 0);
		if (type == MapType.TIME_SORTED) {
			map = new AdaptiveMapUserSuppliedKey(client, getNamespace(), getSetName(), binName, mapPolicy, 1000);
		}
		else {
			map = new AdaptiveMap(client, getNamespace(), getSetName(), binName, null, false, 1000, false);
		}

		if (key != null) {
			System.out.printf("key %s has %d members\n", key, map.countAll(null, key));
			if (explain) {
				if (type == MapType.NORMAL) {
					drawSplits(client, new Key(getNamespace(), getSetName(), key), binName);
				}
				else {
					drawCounts(client, new Key(getNamespace(), getSetName(), key), binName);
				}
			}
		}
		else {
			// Query all the data in the set. Since the keys are not necessarily stored, we need to do a count of all the items in the 
			// set which have a map associated with them.
			// This approach is very costly in terms of network traffic. An aggregation function would be better but this requires different
			// security permissions on the server and might lower the utility of this function
			long count = 0;
			if (useUdf) {
				count = countViaUdf(client, getNamespace(), getSetName(), binName, shouldRegisterUdf);
			}
			else {
				AtomicLong counter = new AtomicLong();
				ScanPolicy scanPolicy = new ScanPolicy();
				scanPolicy.concurrentNodes = true;
				scanPolicy.maxConcurrentNodes = 0;
				client.scanAll(scanPolicy, getNamespace(), getSetName(), new ScanCallback() {
					
					@Override
					public void scanCallback(Key key, Record record) throws AerospikeException {
						counter.addAndGet(record.getMap(binName).size());
					}
				}, binName);
				count = counter.get();
			}
			System.out.printf("Total records in set %s: %,d\n", getSetName(), count);
		}

	}
}