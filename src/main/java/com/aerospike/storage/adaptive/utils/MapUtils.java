package com.aerospike.storage.adaptive.utils;

import java.util.Arrays;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class MapUtils {

	public static void usage(Options options) {
		if (options == null) {
			System.out.printf("MapUtils <command> <args>\n");
			System.out.printf("\tcommand is one of: ");
			for (CommandType thisCommand : CommandType.values()) {
				System.out.printf("%s ", thisCommand.getName());
			}
			System.out.println();
			System.exit(-1);
		}
		else {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.setWidth(120);
			helpFormatter.printHelp("MapUtils", options);
		}
		System.exit(-1);
	}
		
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			usage(null);
		}
		else {
			try {
				CommandType commandType = CommandType.getCommand(args[0]);
				String[] arguments = Arrays.copyOfRange(args, 1, args.length);
				commandType.getCommand().run(commandType, arguments);
				return;
			}
			catch (IllegalArgumentException iae) {
				System.out.println(iae.getMessage());
				usage(null);
			}
		}
	}
}
