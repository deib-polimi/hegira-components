/**
 * 
 */
package it.polimi.hegira;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;

import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.queue.ServiceQueue;
import it.polimi.hegira.queue.ServiceQueueMessage;
import it.polimi.hegira.utils.CLI;
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
			serviceQueue.announcePresence();
			
			while(true){
				
				ServiceQueueMessage sqm = serviceQueue.receiveCommands();
				
				switch(sqm.getCommand()){
					case "switchover":
						if(cli.componentType.equals("SRC")){
							log.debug("Received command message, destined to: SRC");
		            		}else if(cli.componentType.equals("TWC")){
		            			log.debug("Received command message, destined to: TWC");
		            		}
						
						break;
					default:
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
