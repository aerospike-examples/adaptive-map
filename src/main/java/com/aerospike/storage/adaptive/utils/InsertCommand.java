package com.aerospike.storage.adaptive.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.storage.adaptive.AdaptiveMap;
import com.aerospike.storage.adaptive.AdaptiveMapUserSuppliedKey;
import com.aerospike.storage.adaptive.IAdaptiveMap;
import com.aerospike.storage.adaptive.utils.PerformanceTest.MapType;

/**
 * Insert an item into an adaptive map. For example:
 * <pre>java -jar adaptive-map-1.0-full.jar insert -n test -s testSet -b map -k key1 -c 5 -d "{\"key\":\"128\",\"value\":[\"a\",\"b\",1,2,3]}"</pre>
 * <p>
 * Note that the key and value is passed as a JSON object so the code can differentiate between integer and string keys for example. The value can be any valid JSON
 * type like strings, integers, lists, maps, etc.
 * @author timfaulkes
 *
 */
public class InsertCommand extends Command {

	private String binName;
	private String key;
	private int count;
	private String data;
	private Options options;
	private MapType type = MapType.NORMAL;
	
	@Override
	protected void addSubCommandOptions(Options options) {
		options.addRequiredOption("b", "bin", true, "Specifies the bin which contains the map data. (REQUIRED)");
		options.addRequiredOption("k", "key", true, "Key to insert.");
		options.addOption("c", "count", true, "Set the adaptive map count size for this insert. Default: 100");
		options.addRequiredOption("d", "data", true, "Set the key and value for the insert. (REQUIRED)");
		options.addOption("T", "type", true, "Specify the map type (TimeSorted or Normal). Default: Normal");
		this.options = options;
	}
	
	@Override
	protected void extractSubCommandOptions(CommandLine commandLine) {
		this.binName = commandLine.getOptionValue("bin");
		this.key = commandLine.getOptionValue("key");
		this.count = Integer.valueOf(commandLine.getOptionValue("count", "100"));
		this.data = commandLine.getOptionValue("data");
		this.type = MapType.getMapType(commandLine.getOptionValue("type"));
	}
	
	@Override
	protected void run(CommandType commandType, String[] argments) {
		super.parseCommandLine(commandType.getName(), argments);
		IAerospikeClient client = super.connect();
		IAdaptiveMap map;
		MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, 0);
		if (type == MapType.TIME_SORTED) {
			map = new AdaptiveMapUserSuppliedKey(client, getNamespace(), getSetName(), binName, mapPolicy, count);
		}
		else {
			map = new AdaptiveMap(client, getNamespace(), getSetName(), binName, null, false, count);
		}

		// Parse the data
		try {
			JSONObject jsonData = (JSONObject) new JSONParser().parse(data);
			Object keyData = jsonData.get("key");
			Object valueData = jsonData.get("data");
			if (keyData == null || valueData == null) {
				System.out.println("Both key and data must be specified in the JSON");
				usage(CommandType.INSERT.getName(), options);
			}
			else {
//				System.out.println(keyData);
//				System.out.println(valueData);
				map.put(null, key, keyData, null, Value.get(valueData));
			}
		}
		catch (ParseException pe) {
			System.out.println("Invalid JSON in data: " + data + ". Error was " + pe.getMessage());
			usage(CommandType.INSERT.getName(), options);
		}
	}
}