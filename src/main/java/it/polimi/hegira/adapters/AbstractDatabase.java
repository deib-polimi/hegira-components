package it.polimi.hegira.adapters;

import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.utils.Constants;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public abstract class AbstractDatabase implements Runnable{
	//protected TaskQueue taskQueue;
	protected ArrayList<TaskQueue> taskQueues;
	private transient Logger log = Logger.getLogger(AbstractDatabase.class);
	protected int THREADS_NO = 0;
	//protected int thread_id;
	
	/**
	* Constructs a general database object
	* @param options A map containing the properties of the new db object to be created.
	* 			<code>mode</code> Consumer or Producer.
	* 			<code>threads</code> The number of consumer threads.
	* 			<code>queue-address</code>
	*/
	protected AbstractDatabase(Map<String, String> options){
		try {
			switch(options.get("mode")){
				case Constants.PRODUCER:
					taskQueues=new ArrayList<TaskQueue>(1);
					taskQueues.add(new TaskQueue(options.get("mode"), 0, 
							options.get("queue-address")));
					break;
				case Constants.CONSUMER:
					int threads=10;
					if(options.get("threads")!=null){
						threads = Integer.parseInt(options.get("threads"));
						taskQueues=new ArrayList<TaskQueue>(threads);
					}else
						taskQueues=new ArrayList<TaskQueue>(threads);

					this.THREADS_NO=threads;
					for(int i=0;i<threads;i++)
						taskQueues.add(new TaskQueue(options.get("mode"), 
							Integer.parseInt(options.get("threads")), 
							options.get("queue-address")));
					break;
				default:
					log.error(Thread.currentThread().getName()+
							"Unsuported mode: "+options.get("mode"));
					return;
			}
			
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
	 * @param component
	 * @return
	 */
	public final boolean switchOver(String component){
		final AbstractDatabase thiz = this;
		
		if(component.equals("SRC")){
			(new Thread() {
				@Override public void run() {
					//thread_id=0;
					try {
						thiz.connect();
						toMyModel(thiz);
					} catch (ConnectException e) {
						e.printStackTrace();
					} finally{
						thiz.disconnect();
					}     
				}
			}).start();
		}else if(component.equals("TWC")){
			//executing the consumers
			ExecutorService executor = Executors.newFixedThreadPool(thiz.THREADS_NO);
			log.debug("EXECUTOR switchover No. Consumer threads: "+thiz.THREADS_NO);
			for(int i=0;i<thiz.THREADS_NO;i++){
				//thread_id=i;
				executor.execute(thiz);
			}
		}
		return true;
	}
	
	@Override
	public void run() {
		try {
			this.connect();
			log.debug("Starting consumer thread");
			this.fromMyModel(null);
		} catch (ConnectException e) {
			log.error(Thread.currentThread().getName() +
					" Unable to connect to the destination database!", e);
		}	
	}

}
