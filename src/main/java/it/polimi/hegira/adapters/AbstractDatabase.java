package it.polimi.hegira.adapters;

import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.PropertiesManager;
import it.polimi.hegira.vdp.VdpUtils;
import it.polimi.hegira.zkWrapper.MigrationStatus;
import it.polimi.hegira.zkWrapper.MigrationStatus.VDPstatus;
import it.polimi.hegira.zkWrapper.ZKclient;
import it.polimi.hegira.zkWrapper.ZKserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public abstract class AbstractDatabase implements Runnable{
	//protected TaskQueue taskQueue;
	protected ArrayList<TaskQueue> taskQueues;
	private transient Logger log = Logger.getLogger(AbstractDatabase.class);
	protected int THREADS_NO = 0;
	//protected int thread_id;
	
	//String tableName
	//MigrationStatus the migration status for that table
	protected HashMap<String, MigrationStatus> snapshot;
	protected int vdpSize;
	String connectString = PropertiesManager.getZooKeeperConnectString();
	//ugly but for prototyping...
	//(normal || partitioned)
	private String behavior = "normal"; 
	
	/**
	 * VDPsCounters
	 * K1 tableName
	 * V1-K2 VDPid
	 * V1-V2 VDP counter 
	 */
	protected ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> VDPsCounters;
	
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
					snapshot = new HashMap<String, MigrationStatus>();
					break;
				case Constants.CONSUMER:
					int threads=10;
					if(options.get("threads")!=null)
						threads = Integer.parseInt(options.get("threads"));
					
					taskQueues=new ArrayList<TaskQueue>(threads);

					this.THREADS_NO=threads;
					for(int i=0;i<threads;i++)
						taskQueues.add(new TaskQueue(options.get("mode"), 
							Integer.parseInt(options.get("threads")), 
							options.get("queue-address")));
					
					VDPsCounters = new ConcurrentHashMap<String, 
							ConcurrentHashMap<Integer,Integer>>(10, 0.75f, threads);
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
	* Encapsulate the logic contained inside the models to map to the intermediate model
	* to a DB in a partitioned way.
	* @param mm The intermediate model
	* @return returns the converted model
	*/
	protected abstract AbstractDatabase fromMyModelPartitioned(Metamodel mm);
	
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
	 * Encapsulate the logic contained inside the models to map a DB to the intermediate
	 * model, reading from the source database in a virtually partitioned way
	 * @param model
	 * @return
	 */
	protected abstract Metamodel toMyModelPartitioned(AbstractDatabase model);
	
	/**
	 * Template method
	 * @param component
	 * @return
	 */
	public final boolean switchOver(String component){
		final AbstractDatabase thiz = this;
		behavior = "normal";
		
		if(component.equals("SRC")){
			(new Thread() {
				@Override public void run() {
					//thread_id=0;
					try {
						thiz.connect();
						thiz.toMyModel(thiz);
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
	
	/**
	 * Template method
	 * Performs the switchOver in a virtually partitioned way.
	 * The TWC part is unchanged with respect to the switchOver method.
	 * @param component The name of the component (i.e., SRC or TWC)
	 * @return
	 */
	public final boolean switchOverPartitioned(String component){
		final AbstractDatabase thiz = this;
		behavior = "partitioned";
		
		if(component.equals("SRC")){
			(new Thread() {
				@Override public void run() {
					//thread_id=0;
					try {
						thiz.connect();
						thiz.createSnapshot(thiz.getTableList());
						thiz.toMyModelPartitioned(thiz);
					} catch (ConnectException e) {
						e.printStackTrace();
					} catch (Exception e) {
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
			if(behavior.equals("normal"))
				this.fromMyModel(null);
			else if(behavior.equals("partitioned")){
				this.fromMyModelPartitioned(null);
			}
		} catch (ConnectException e) {
			log.error(Thread.currentThread().getName() +
					" Unable to connect to the destination database!", e);
		}	
	}

	

	/**
	 * Creates a new snapshot of the source database by considering just 
	 * the list of tables given as input.
	 * Notice that the previous snapshot data will be deleted.
	 * @param tablesList The list of tables that will be considered to create the snapshot.
	 * @throws Exception ZK errors, interruptions, etc.
	 */
	protected void createSnapshot(List<String> tablesList) throws Exception{
		//String connectString = PropertiesManager.getZooKeeperConnectString();
		ZKclient zKclient = new ZKclient(connectString);
		ZKserver zKserver = new ZKserver(connectString);
		//set the queryLock used as a flag to block query propagation 
		//until the snapshot has been completely created.
		zKserver.lockQueries();
		
		try {
			//getting the VDPsize
			vdpSize = zKserver.getVDPsize();
			for(String tbl : tablesList){
				int seqNr = zKclient.getCurrentSeqNr(tbl);
				int totalVDPs = VdpUtils.getTotalVDPs(seqNr, vdpSize);
				//automatically setting the VDPStatus to NOT_MIGRATED
				MigrationStatus status = new MigrationStatus(seqNr, totalVDPs);
				boolean setted = zKserver.setMigrationStatus(tbl, status);
				snapshot.put(tbl, status);
			}
		} catch (Exception e) {
			log.error(Thread.currentThread().getName() +
					" Unable to create a snapshot!", e);
		}
		
		//release the queryLock
		zKserver.unlockQueries();
		zKserver.close();
	}
	
	public void updateMigrationStatus(String tableName, int VDPid, VDPstatus status) throws Exception{
		ZKserver zKserver = new ZKserver(connectString);
		MigrationStatus migrationStatus = snapshot.get(tableName);
		migrationStatus.updateVDP(VDPid, status);
		zKserver.setMigrationStatus(tableName, migrationStatus);
		zKserver.close();
		zKserver=null;
	}
	
	/**
	 * Returns a list containing all the tables of the database.
	 * @return The list of tables.
	 */
	public abstract List<String> getTableList();
}
