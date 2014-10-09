package it.polimi.hegira.adapters;

import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;

import java.util.Map;

public abstract class AbstractDatabase implements Runnable{
	private TaskQueue taskQueue;
	
	/**
	* Constructs a general database object
	* @param options A map containing the properties of the new db object to be created.
	* 			<code>mode</code> Consumer or Producer.
	* 			<code>threads</code> The number of consumer threads.
	* 			<code>queue-address</code>
	*/
	protected AbstractDatabase(Map<String, String> options){
		try {
			taskQueue = new TaskQueue(options.get("mode"), Integer.parseInt(options.get("threads")), 
					options.get("queue-address"));
		} catch (NumberFormatException | QueueException e) {
			e.printStackTrace();
			return;
		}
	}
	
	/**
	* Encapsulate the logic contained inside the models to map to the intermediate model
	* to a DB
	* @param mm The intermediate model
	* @return returns the converted model
	*/
	protected abstract AbstractDatabase fromMyModel(Metamodel mm);
	/**
	* Encapsulate the logic contained inside the models to map a DB to the intermediate
	* model
	* @param model The model to be converted
	* @return returns The intermediate model
	*/
	protected abstract Metamodel toMyModel(AbstractDatabase model);
	/**
	* Suggestion: To be called inside a try-finally block. Should always be disconnected
	* @throws ConnectException
	*/
	public abstract void connect() throws ConnectException;
	/**
	* Suggestion: To be called inside a finally block
	*/
	public abstract void disconnect();
	
	/**
	 * Template method
	 * @param destination
	 * @return
	 */
	public final boolean switchOver(AbstractDatabase destination){
		return false;
		//TODO implement switchover
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
