package com.aerospike.storage.adaptive.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.storage.adaptive.AdaptiveMap;

/**
 * Implement a delete from an adaptive map. For example:
 * <pre>java -jar adaptive-map-1.0-full.jar delete -n test -s testSet -b map -k key1 -c 5 -d "{\"key\":\"1\"}"</pre>
 * <p>
 * Note that the key is passed as a JSON object so the code can differentiate between integer and string keys for example.
 * @author timfaulkes
 *
 */
public class DeleteCommand extends Command {

	private String binName;
	private String key;
	private int count;
	private Options options;
	private String data;
	
	@Override
	protected void addSubCommandOptions(Options options) {
		options.addRequiredOption("b", "bin", true, "Specifies the bin which contains the map data. (REQUIRED)");
		options.addRequiredOption("k", "key", true, "Key to insert.");
		options.addOption("c", "count", true, "Set the adaptive map count size for this delete. Default: 100");
		options.addRequiredOption("d", "data", true, "Set the key and value for the delete. (REQUIRED)");
		this.options = options;
	}
	
	@Override
	protected void extractSubCommandOptions(CommandLine commandLine) {
		this.binName = commandLine.getOptionValue("bin");
		this.key = commandLine.getOptionValue("key");
		this.count = Integer.valueOf(commandLine.getOptionValue("count", "100"));
		this.data = commandLine.getOptionValue("data");
	}
	
	@Override
	protected void run(CommandType type, String[] argments) {
		super.parseCommandLine(type.getName(), argments);
		IAerospikeClient client = super.connect();
		AdaptiveMap map = new AdaptiveMap(client, getNamespace(), getSetName(), binName, null, false, count);

		// Parse the data
		try {
			JSONObject jsonData = (JSONObject) new JSONParser().parse(data);
			Object keyData = jsonData.get("key");
			if (keyData == null) {
				System.out.println("Key must be specified in the JSON");
				usage(CommandType.DELETE.getName(), options);
			}
			else {
				map.delete(null, key, keyData, null);
			}
		}
		catch (ParseException pe) {
			System.out.println("Invalid JSON in data: " + data + ". Error was " + pe.getMessage());
			usage(CommandType.DELETE.getName(), options);
		}
	}
}