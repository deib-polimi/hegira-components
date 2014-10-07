/**
 * 
 */
package it.polimi.hegira.queue;

import java.util.List;

/**
 * Class defining the structure of a command message passed in the service queue.
 * (To be serialized with the standard Java serialization {@link it.polimi.hegira.utils.DefaultSerializer})
 * @author Marco Scavuzzo
 */
public class ServiceQueueMessage {
	private String source;
	private List<String> destination;
	private int threads;
	private String command;
	
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public List<String> getDestination() {
		return destination;
	}
	public void setDestination(List<String> destination) {
		this.destination = destination;
	}
	public int getThreads() {
		return threads;
	}
	public void setThreads(int threads) {
		this.threads = threads;
	}
}
