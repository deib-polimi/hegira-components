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
package it.polimi.hegira;

import java.util.HashMap;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.adapters.DatabaseFactory;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.queue.ServiceQueue;
import it.polimi.hegira.queue.ServiceQueueMessage;
import it.polimi.hegira.utils.CLI;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;

/**
 * Hegira-components entry point.
 * @author Marco Scavuzzo
 */
public class EntryClass {
	private static Logger log = Logger.getLogger(EntryClass.class);
	
	public static void main(String[] args) {
		//PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("log.properties").getFile());
		
		CLI cli = new CLI();
		JCommander jc = new JCommander(cli);
		try{
			jc = new JCommander(cli,args);
		}catch(Exception e){
			jc.usage();
			return;
		}
			
		ServiceQueue serviceQueue = new ServiceQueue(cli.componentType, cli.queueAddress);
		try {
			//Telling hegira-api that we are ready to receive commands
			try{
				serviceQueue.announcePresence();
			}catch(QueueException | NullPointerException ex){
				log.error("Unable to connect to the Queue. The program threw the following exception: ", ex);
				return;
			}
			
			/**
			 * Continuously waiting for command messages.
			 * Actions to message should be executed by threads, 
			 * in order for the application to be responsive (to other commands, 
			 * 	i.e stop-migration, report-status, ecc..). 
			 */
			while(true){
				
				ServiceQueueMessage sqm = serviceQueue.receiveCommands();
				
				switch(sqm.getCommand()){
					case "switchover":
						if(cli.componentType.equals("SRC")){
							log.debug("Received command message, destined to: SRC");
							HashMap<String, String> options_producer = new HashMap<String,String>();
							options_producer.put("mode", Constants.PRODUCER);
							if(sqm.getSRTs_NO()>=1)
								options_producer.put("SRTs_NO", ""+sqm.getSRTs_NO());
							
							options_producer.put("queue-address", cli.queueAddress);
							AbstractDatabase src = DatabaseFactory.getDatabase(sqm.getSource(), options_producer);
							src.switchOver("SRC");
		            		}else if(cli.componentType.equals("TWC")){
		            			log.debug("Received command message, destined to: TWC");
		            			HashMap<String, String> options_consumer = new HashMap<String,String>();
		        				options_consumer.put("mode", Constants.CONSUMER);
		        				if(sqm.getThreads()>=1){
		        					options_consumer.put("threads", ""+sqm.getThreads());
		        				}
		        				
		        				options_consumer.put("queue-address", cli.queueAddress);
		        				AbstractDatabase dst = DatabaseFactory.getDatabase(sqm.getDestination().get(0),
		        						options_consumer);
		        				dst.switchOver("TWC");
		            		}
						
						//Telling hegira-api that we are ready to receive other commands
						serviceQueue.announcePresence();
						break;
					case "switchoverPartitioned":
						if(cli.componentType.equals("SRC")){
							log.debug("Received command message, destined to: SRC");
							HashMap<String, String> options_producer = new HashMap<String,String>();
							options_producer.put("mode", Constants.PRODUCER);
							options_producer.put("queue-address", cli.queueAddress);
							
							AbstractDatabase src = DatabaseFactory.getDatabase(sqm.getSource(), options_producer);
							src.switchOverPartitioned("SRC", false);
		            		}else if(cli.componentType.equals("TWC")){
		            			log.debug("Received command message, destined to: TWC");
		            			HashMap<String, String> options_consumer = new HashMap<String,String>();
		        				options_consumer.put("mode", Constants.CONSUMER);
		        				if(sqm.getThreads()>=1){
		        					options_consumer.put("threads", ""+sqm.getThreads());
		        				}
		        				options_consumer.put("queue-address", cli.queueAddress);
		        				
		        				AbstractDatabase dst = DatabaseFactory.getDatabase(sqm.getDestination().get(0),
		        						options_consumer);
		        				dst.switchOverPartitioned("TWC",false);
		            		}
						
						//Telling hegira-api that we are ready to receive other commands
						serviceQueue.announcePresence();
						break;
						
					case "recover":
						if(cli.componentType.equals("SRC")){
							//Reads the MigrationStatus from ZooKeeper and rebuilds a local status from which to start migrating
							log.debug("Received command message, destined to: SRC");
							HashMap<String, String> options_producer = new HashMap<String,String>();
							options_producer.put("mode", Constants.PRODUCER);
							options_producer.put("queue-address", cli.queueAddress);
							
							AbstractDatabase src = DatabaseFactory.getDatabase(sqm.getSource(), options_producer);
							src.switchOverPartitioned("SRC", true);
						}else if(cli.componentType.equals("TWC")){
							//shouldn't be necessary to adjust the counters for the entities correctly migrated (expected 
							//number is piggybacked in the metamodel entity) since an already started VDP is recovered anyway
							
							log.debug("Received command message, destined to: TWC");
		            			HashMap<String, String> options_consumer = new HashMap<String,String>();
		        				options_consumer.put("mode", Constants.CONSUMER);
		        				if(sqm.getThreads()>=1){
		        					options_consumer.put("threads", ""+sqm.getThreads());
		        				}
		        				options_consumer.put("queue-address", cli.queueAddress);
		        				
		        				AbstractDatabase dst = DatabaseFactory.getDatabase(sqm.getDestination().get(0),
		        						options_consumer);
		        				dst.switchOverPartitioned("TWC",true);
						}
						//Telling hegira-api that we are ready to receive other commands
						serviceQueue.announcePresence();
						break;
					default:
						log.debug("Received command message: "+sqm.getCommand());
						//Telling hegira-api that we are ready to receive other commands
						serviceQueue.announcePresence();
						break;
				}
			}
		} catch (QueueException e) {
			log.error(DefaultErrors.queueError);
			log.error(e.getMessage());
			return;
		}
	}

}
