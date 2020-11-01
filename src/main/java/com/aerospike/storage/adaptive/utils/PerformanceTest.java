package com.aerospike.storage.adaptive.utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.storage.adaptive.AdaptiveMap;
import com.aerospike.storage.adaptive.AdaptiveMapUserSuppliedKey;
import com.aerospike.storage.adaptive.IAdaptiveMap;

public class PerformanceTest extends Command {
	
	public static enum MapType {
		TIME_SORTED("TimeSorted"),
		NORMAL("Normal");
		
		private String type;
		private MapType(String type) {
			this.type = type;
		}
		public String getType() {
			return type;
		}
		public static MapType getMapType(String type) {
			for (MapType aType : MapType.values()) {
				if (aType.getType().equals(type)) {
					return aType;
				}
			}
			throw new IllegalArgumentException("Map Type " + type + " does not exist.");
		}
	}
	
	private String binName;
	private int threads = 1;
	private int numKeys = 1000;
	private int totalKeys = 1_000_000;
	private int blockSplitSize = 100;
	private MapType type = MapType.NORMAL;
	
	private AtomicLong counter = new AtomicLong();
	private AtomicLong errorCounter = new AtomicLong();
	private AtomicLong keyCounter = new AtomicLong();
	private AtomicLongArray lastWriteTime = new AtomicLongArray(threads);
	private volatile boolean terminate = false;
	
	@Override
	protected void addSubCommandOptions(Options options) {
		options.addRequiredOption("b", "bin", true, "Specifies the bin which contains the map data. (REQUIRED)");
		options.addOption("t", "threads", true, "Specify the number of threads. Default: 1");
		options.addOption("c", "count", true, "Set the adaptive map count size for this insert. Default: 100");
		options.addOption("T", "type", true, "Specify the map type (TimeSorted or Normal). Default: Normal");
	}
	
	@Override
	protected void extractSubCommandOptions(CommandLine commandLine) {
		this.binName = commandLine.getOptionValue("bin");
		this.threads = Integer.valueOf(commandLine.getOptionValue("threads"));
		this.blockSplitSize = Integer.valueOf(commandLine.getOptionValue("count", "100"));
		this.type = MapType.getMapType(commandLine.getOptionValue("type"));
	}
	
	public String getKey(long id) {
		String numberKey = "0000000000" + id; 
		return "key" + (numberKey.substring(numberKey.length()-10));
	}

	public ExecutorService executors = Executors.newFixedThreadPool(threads);
	
	public class Runner implements Runnable {
		private final IAdaptiveMap map;
		private final int id;
		private final Random random;
		
		public Runner(IAdaptiveMap map, int id) {
			this.map = map;
			this.id = id;
			this.random = new Random(id);
		}
		
		@Override
		public void run() {
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.maxRetries = 5;
			writePolicy.sleepBetweenRetries = 200;
			writePolicy.totalTimeout = 5000;
			while (!terminate) {
				int key = random.nextInt(numKeys);
				//int mapKey = random.nextInt();
				long mapKey = keyCounter.incrementAndGet();
				try {
					map.put(writePolicy, "Key-" + key, mapKey, null, Value.get(counter.get()));
					lastWriteTime.set(id, System.nanoTime());
					counter.incrementAndGet();
				}
				catch (Exception e) {
					errorCounter.incrementAndGet();
					lastWriteTime.set(id, System.nanoTime());
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Get the health of the threads. If a thread hasn't processed a transaction in more than 1 second, it is flagged 
	 * as a problem thread.
	 * @param now
	 * @return
	 */
	public String getThreadHealth(long now) {
		StringBuffer buffer = new StringBuffer(threads);
		for (int i = 0; i < threads; i++) {
			if (now - lastWriteTime.get(i) > 1_000_000_000) {
				buffer.append('X');
			}
			else {
				buffer.append('.');
			}
		}
		return buffer.toString();
	}
	
	@Override
	protected void run(CommandType type, String[] argments) throws Exception {
//		client.truncate(null, NAMESPACE, SET, null);
		IAdaptiveMap map;
		MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, 0);
		if (this.type == MapType.NORMAL) {
			map = new AdaptiveMap(getAerospikeClient(), getNamespace(), getSetName(), 
					binName, mapPolicy, false, blockSplitSize);
		}
		else {
			map = new AdaptiveMapUserSuppliedKey(getAerospikeClient(), getNamespace(), getSetName(), 
					binName, mapPolicy, blockSplitSize);
		}
		for (int i = 0; i < threads; i++) {
			executors.submit(new Runner(map, i));
		}
		executors.shutdown();
		long start = System.nanoTime();
		while (true) {
			Thread.sleep(1000);
			long now = System.nanoTime();
			System.out.printf("%,9dms: Success: %,9d  Failed: %,9d  Thread Health: %s\n", (now-start)/1_000_000, counter.get(), errorCounter.get(), getThreadHealth(now));
			if (counter.get() >= totalKeys) {
				terminate = true;
				executors.awaitTermination(1, TimeUnit.DAYS);
				break;
			}
		}
		Thread.sleep(2000);
		System.out.printf("Done: Success: %,9d  Failed: %,9d\n", counter.get(), errorCounter.get());
	}
	
}
