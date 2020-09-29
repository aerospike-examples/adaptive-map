package com.aerospike.storage.adaptive.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.storage.adaptive.AdaptiveMap;
import com.aerospike.storage.adaptive.BitwiseData;

public class CountRecords {

	private static IAerospikeClient client;
	private static String binName;
	
	public static void usage(Options options) {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.setWidth(120);
		helpFormatter.printHelp("CountRecords", options);
		System.exit(-1);
	}
	
	private static boolean isNonLeafNode(BitwiseData data, int blockNum) {
		return data.get(blockNum);
	}
	
	private static boolean isLeafNode(BitwiseData data, int blockNum) {
		int parentBlock = (blockNum-1)/2;
		return (data.get(blockNum) == false && (blockNum == 0 || data.get(parentBlock) == true));
	}
	
	private static int maxSplits(BitwiseData data, int level, int count) {
		if (data.get(level)) {
			int left = maxSplits(data, 2*level+1, count+1);
			int right = maxSplits(data, 2*level+2, count+1);
			return Math.max(left, right);
		}
		return count;
	}
	
	private static int highestBlock(BitwiseData data, int level) {
		if (data.get(level)) {
			int left = highestBlock(data, 2*level+1);
			int right = highestBlock(data, 2*level+2);
			return Math.max(left, right);
		}
		return level;
	}
	
	private static String getSpaces(int count) {
		String spaces = "";
		for (int i = 0; i < count; i++) {
			spaces += " ";
		}
		return spaces;
	}
	
	private static String padToSize(int number, int size) {
		return padToSize(Integer.toString(number), size);
	}
	
	private static String padToSize(String string, int size) {
		int excess = size - string.length();
		if (excess > 0) {
			return getSpaces(excess / 2) + string + getSpaces(excess - (excess/2));
		}
		else {
			return string;
		}
	}
	
	private static int getMiddleOfBottomBlock(int index, int startBottomBlock, int longestBlockLen) {
		int offset = index - startBottomBlock;
		return (longestBlockLen + 1) * offset + longestBlockLen/2;
	}
	
	private static int getBlockMiddleOffset(int index, int startBottomBlock, int longestBlockLen) {
		if (index >= startBottomBlock) {
			return getMiddleOfBottomBlock(index, startBottomBlock, longestBlockLen);
		}
		else {
			int left = getBlockMiddleOffset(2*index+1, startBottomBlock, longestBlockLen);
			int right = getBlockMiddleOffset(2*index+2, startBottomBlock, longestBlockLen);
			return (left+right)/2;
		}
	}
	
	private static int getStartIndexOfBottomBlock(int maxSplits) {
		return (1<<maxSplits)-1;
	}
	
	private static void drawCountsForLevel(Key key, BitwiseData bitwiseData, int level, int maxSplits, int longestBlockLen) {
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
				Record record = client.operate(null, new Key(key.namespace, key.setName, id), MapOperation.getByIndexRange(binName, 0, MapReturnType.COUNT));
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
	
	private static void drawSplitsForLevel(Key key, BitwiseData bitwiseData, int level, int maxSplits, int longestBlockLen) {
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
	
	private static void drawSplits(IAerospikeClient client, Key key, String mapBin) {
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
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addRequiredOption("n", "namespace", true, "Specifies the namespace to use (REQUIRED)");
		options.addRequiredOption("s", "set", true, "Specifies the set to use. (REQUIRED)");
		options.addRequiredOption("b", "bin", true, "Specifies the bin which contains the map data. (REQUIRED)");
		options.addOption("p", "port", true, "Port of the Aerospike repository (default: 3000)");
		options.addOption("h", "host", true, "Seed host of the Aerospike repository (default: localhost)");
		options.addOption("k", "key", true, "Count sub-items associated with a specific record. If not provided, all records will be counted.");
		options.addOption("e", "explain", false, "Explain the splits and counts for a single key record. Ignored if --key is not specified.");
		options.addOption("U", "user", true, "Specify the user for security enabled clusters");
		options.addOption("P", "password", true, "Specify the password for security enabled clusters");
		
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
		}
		catch (ParseException pe) {
			System.out.printf("Invalid usage: %s\n", pe.getMessage());
			usage(options);
		}
		
		String namespace = commandLine.getOptionValue("namespace");
		int port = Integer.parseInt(commandLine.getOptionValue("port", "3000"));
		String host = commandLine.getOptionValue("host", "localhost");
		String setName = commandLine.getOptionValue("set");
		binName = commandLine.getOptionValue("bin");
		String key = commandLine.getOptionValue("key");
		boolean explain = commandLine.hasOption("explain");
		String user = commandLine.getOptionValue("user");
		String password = commandLine.getOptionValue("password");
		
		System.out.printf("Options: Host = %s, port = %d, Namespace = %s\n", host, port, namespace);
		ClientPolicy policy = new ClientPolicy();
		policy.user = user;
		policy.password = password;
		client = new AerospikeClient(policy, host, port);
		
		AdaptiveMap map = new AdaptiveMap(client, namespace, setName, binName, null, false, 1000, false);
		if (key != null) {
			System.out.printf("key %s has %d members\n", key, map.countAll(null, key));
			if (explain) {
				drawSplits(client, new Key(namespace, setName, key), binName);
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
			client.scanAll(scanPolicy, namespace, setName, new ScanCallback() {
				
				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {
					counter.addAndGet(record.getMap(binName).size());
				}
			}, binName);
			System.out.printf("Total records in set %s: %,d\n", setName, counter.get());
		}
	}
}
