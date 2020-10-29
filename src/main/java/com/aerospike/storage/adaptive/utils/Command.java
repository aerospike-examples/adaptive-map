package com.aerospike.storage.adaptive.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

public abstract class Command {
	private String namespace;
	private int port;
	private String host;
	private String setName;
	private String user;
	private String password;
	private IAerospikeClient client;
	
	protected abstract void run(CommandType type, String[] argments) throws Exception;
	
	protected void addSubCommandOptions(Options options) {}
	protected void extractSubCommandOptions(CommandLine commandLine) {}
	
	protected static void usage(String name, Options options) {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.setWidth(120);
		helpFormatter.printHelp("MapUtils " + name + " <args>", options);
		System.exit(-1);
	}
	
	protected void parseCommandLine(String name, String[] arguments) {
		Options options = new Options();
		addCommonOptions(options);
		addSubCommandOptions(options);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, arguments);
		}
		catch (ParseException pe) {
			System.out.printf("Invalid usage: %s\n", pe.getMessage());
			usage(name, options);
		}
		
		extractCommonOptions(commandLine);
		extractSubCommandOptions(commandLine);
	}
	
	private void addCommonOptions(Options options) {
		options.addRequiredOption("n", "namespace", true, "Specifies the namespace to use (REQUIRED)");
		options.addRequiredOption("s", "set", true, "Specifies the set to use. (REQUIRED)");
		options.addOption("p", "port", true, "Port of the Aerospike repository (default: 3000)");
		options.addOption("h", "host", true, "Seed host of the Aerospike repository (default: localhost)");
		options.addOption("U", "user", true, "Specify the user for security enabled clusters");
		options.addOption("P", "password", true, "Specify the password for security enabled clusters");
	}
	
	private void extractCommonOptions(CommandLine commandLine) {
		this.namespace = commandLine.getOptionValue("namespace");
		this.port = Integer.parseInt(commandLine.getOptionValue("port", "3000"));
		this.host = commandLine.getOptionValue("host", "localhost");
		this.setName = commandLine.getOptionValue("set");
		this.user = commandLine.getOptionValue("user");
		this.password = commandLine.getOptionValue("password");
	}
	
	public String getNamespace() {
		return this.namespace;
	}
	
	public String getSetName() {
		return this.setName;
	}
	
	public int getPort() {
		return port;
	}
	public String getHost() {
		return host;
	}
	public String getUser() {
		return user;
	}
	public String getPassword() {
		return password;
	}
	
	public IAerospikeClient connect() {
		ClientPolicy policy = new ClientPolicy();
		policy.user = user;
		policy.password = password;
		client = new AerospikeClient(policy, host, port);
		return client;
	}
	
	public IAerospikeClient getAerospikeClient() {
		if (client == null) {
			return connect();
		}
		return client;
	}
}