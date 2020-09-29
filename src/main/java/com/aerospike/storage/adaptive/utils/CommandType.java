package com.aerospike.storage.adaptive.utils;

public enum CommandType {
	COUNT("count", CountCommand.class),
	INSERT("insert", InsertCommand.class),
	DELETE("delete", DeleteCommand.class);
	
	private String name;
	private Class<? extends Command> commandClazz;
	private CommandType(String commandName, Class<? extends Command> command) {
		this.name = commandName;
		this.commandClazz = command;
	}
	
	public String getName() {
		return name;
	}
	
	public Command getCommand() throws InstantiationException, IllegalAccessException {
		return commandClazz.newInstance();
	}
	
	public static CommandType getCommand(String commandName) {
		for (CommandType type : values()) {
			if (type.getName().equals(commandName)) {
				return type;
			}
		}
		throw new IllegalArgumentException("Unknown command name '" + commandName + "'");
	}
}