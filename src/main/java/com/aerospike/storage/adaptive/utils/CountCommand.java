package com.aerospike.storage.adaptive.utils;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.storage.adaptive.AdaptiveMap;
import com.aerospike.storage.adaptive.BitwiseData;

/**
 * Count the number of records
 * <pre>java -jar adaptive-map-1.0-full.jar count -n test -s testSet -b map -k key1 -e</pre>
 * @author timfaulkes
 *
 */
public class CountCommand extends Command {

	private String binName;
	private String key;
	private boolean explain;
	
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
	
	private String getSpaces(int count) {
		String spaces = "";
		for (int i = 0; i < count; i++) {
			spaces += " ";
		}
		return spaces;
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

	@Override
	protected void addSubCommandOptions(Options options) {
		options.addRequiredOption("b", "bin", true, "Specifies the bin which contains the map data. (REQUIRED)");
		options.addOption("k", "key", true, "Count sub-items associated with a specific record. If not provided, all records will be counted.");
		options.addOption("e", "explain", false, "Explain the splits and counts for a single key record. Ignored if --key is not specified.");
	}
	
	@Override
	protected void extractSubCommandOptions(CommandLine commandLine) {
		this.binName = commandLine.getOptionValue("bin");
		this.key = commandLine.getOptionValue("key");
		this.explain = commandLine.hasOption("explain");
	}
	
	@Override
	protected void run(CommandType type, String[] argments) {
		super.parseCommandLine(type.getName(), argments);
		IAerospikeClient client = super.connect();
		
		AdaptiveMap map = new AdaptiveMap(client, getNamespace(), getSetName(), binName, null, false, 1000, false);
		if (key != null) {
			System.out.printf("key %s has %d members\n", key, map.countAll(null, key));
			if (explain) {
				drawSplits(client, new Key(getNamespace(), getSetName(), key), binName);
			}
		}
		else {
			// Query all the data in the set. Since the keys are not necessarily stored, we need to do a count of all the items in the 
			// set which have a map associated with them.
			// This approach is very costly in terms of network traffic. An aggregation function would be better but this requires different
			// security permissions on the server and might lower the utility of this function
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
			System.out.printf("Total records in set %s: %,d\n", getSetName(), counter.get());
		}

	}
}