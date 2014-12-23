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
							
							AbstractDatabase src = DatabaseFactory.getDatabase(sqm.getSource(), options_producer);
							src.switchOver("SRC");
		            		}else if(cli.componentType.equals("TWC")){
		            			log.debug("Received command message, destined to: TWC");
		            			HashMap<String, String> options_consumer = new HashMap<String,String>();
		        				options_consumer.put("mode", Constants.CONSUMER);
		        				if(sqm.getThreads()>=1){
		        					options_consumer.put("threads", ""+sqm.getThreads());
		        				}
		        				
		        				AbstractDatabase dst = DatabaseFactory.getDatabase(sqm.getDestination().get(0),
		        						options_consumer);
		        				dst.switchOver("TWC");
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
