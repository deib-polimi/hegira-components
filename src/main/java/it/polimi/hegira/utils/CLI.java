/**
 * 
 */
package it.polimi.hegira.utils;

import com.beust.jcommander.Parameter;

/**
 * Defines the parameters needed to launch the program.
 * @author Marco Scavuzzo
 *	TODO: add a parameter to distinguish between the master/slave SRC, in
 *			active/passive mode.
 */
public class CLI {
	
	@Parameter(names = { "--type", "-t" }, description = "launch a SRC or a TWC",
			required = true, validateWith = ComponentValidator.class)
	public String componentType;
	
	@Parameter(names = { "--queue","-q" }, description = "RabbitMQ queue address.")
	public String queueAddress = "localhost";
}
