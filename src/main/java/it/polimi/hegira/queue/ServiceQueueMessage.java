/**
 * Copyright 2015 Marco Scavuzzo
 * Contact: Marco Scavuzzo <marco.scavuzzo@polimi.it>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package it.polimi.hegira.queue;

import java.io.Serializable;
import java.util.List;

/**
 * Class defining the structure of a command message passed in the service queue.
 * (To be serialized with the standard Java serialization {@link it.polimi.hegira.utils.DefaultSerializer})
 * @author Marco Scavuzzo
 */
public class ServiceQueueMessage implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String source;
	private List<String> destination;
	private int threads;
	private String command;
	private int SRTs_NO;
	
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
	public int getSRTs_NO() {
		return SRTs_NO;
	}
	public void setSRTs_NO(int sRTs_NO) {
		SRTs_NO = sRTs_NO;
	}
}
