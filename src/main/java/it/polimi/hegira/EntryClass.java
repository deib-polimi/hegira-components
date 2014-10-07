/**
 * 
 */
package it.polimi.hegira;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;

import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.queue.ServiceQueue;
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
			serviceQueue.announcePresence();
			serviceQueue.receiveCommands();
		} catch (QueueException e) {
			log.error(DefaultErrors.queueError);
			log.error(e.getMessage());
			return;
		}
	}

}
