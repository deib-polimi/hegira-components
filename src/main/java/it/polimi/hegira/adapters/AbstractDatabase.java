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
import it.polimi.hegira.zkWrapper.statemachine.State;
import it.polimi.hegira.zkWrapper.statemachine.StateMachine;
import it.polimi.hegira.utils.VDPsCounters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public abstract class AbstractDatabase implements Runnable{
	//protected TaskQueue taskQueue;
	protected ArrayList<TaskQueue> taskQueues;
	private transient Logger log = Logger.getLogger(AbstractDatabase.class);
	protected int TWTs_NO = 0, SRTs_NO = 1;
	//protected int thread_id;
	
	//String tableName
	//MigrationStatus the migration status for that table
	protected ConcurrentHashMap<String, MigrationStatus> snapshot;
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
	protected VDPsCounters vdpsCounters;
	
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
					snapshot = new ConcurrentHashMap<String, MigrationStatus>();
					int srts_no = 8;
					if(options.get("SRTs_NO")!=null)
						srts_no = Integer.parseInt(options.get("SRTs_NO"));
					this.SRTs_NO = srts_no;
					break;
				case Constants.CONSUMER:
					int threads=10;
					if(options.get("threads")!=null)
						threads = Integer.parseInt(options.get("threads"));
					
					taskQueues=new ArrayList<TaskQueue>(threads);

					this.TWTs_NO=threads;
					for(int i=0;i<threads;i++)
						taskQueues.add(new TaskQueue(options.get("mode"), 
							Integer.parseInt(options.get("threads")), 
							options.get("queue-address")));
					
					vdpsCounters = new VDPsCounters();
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
	 * Start a connection towards the database. 
	 * Suggestion: To be called inside a try-finally block. Should always be disconnected
	 * @throws ConnectException
	 */
	public abstract void connect() throws ConnectException;
	/**
	 * Disconnects and closes all connections to the database.
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
	 * Template method which performs the non-partitioned data migration.
	 * @param component A string representing the component, i.e. SRC or TWC
	 * @return true if the migration has completed successfully from the point of view of the component who called it.
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
			ExecutorService executor = Executors.newFixedThreadPool(thiz.TWTs_NO);
			log.debug("EXECUTOR switchover No. Consumer threads: "+thiz.TWTs_NO);
			for(int i=0;i<thiz.TWTs_NO;i++){
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
	 * @param recover <b>true</b> if the migration should be recovered from the last successfully migrated VDP; <b>false</b> otherwise
	 * @return
	 */
	public final boolean switchOverPartitioned(String component, final boolean recover){
		final AbstractDatabase thiz = this;
		behavior = "partitioned";
		try {
			vdpSize = new ZKserver(connectString).getVDPsize();
			log.debug("Got VDPsize = "+vdpSize);
		} catch (Exception e) {
			log.error("Unable to retrieve VDP size", e);
		}
		
		if(component.equals("SRC")){
			//Creating the snapshot
			try {
				thiz.connect();
				if(!recover){
					log.debug(Thread.currentThread().getName()+
							" creating snapshot...");
					thiz.createSnapshot(thiz.getTableList());
				}else{
					log.debug(Thread.currentThread().getName()+
							" recoverying snapshot...");
					thiz.restoreSnapshot(thiz.getTableList());
				}
			} catch (ConnectException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				thiz.disconnect();
			} 
			
			//parallel data extraction
			for(int i=0; i<SRTs_NO;i++){
				(new Thread() {
					@Override public void run() {
						//thread_id=0;
						try {
							thiz.connect();
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
			}
		}else if(component.equals("TWC")){
			//executing the consumers
			ExecutorService executor = Executors.newFixedThreadPool(thiz.TWTs_NO,
					new TWTFactory());
			log.debug("EXECUTOR switchover No. Consumer threads: "+thiz.TWTs_NO);
			for(int i=0;i<thiz.TWTs_NO;i++){
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
	 * Notice that the previous snapshot data, on ZooKeeper, will be deleted.
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
		log.debug(Thread.currentThread().getName()+
				" locking queries...");
		
		try {
			//getting the VDPsize
			//vdpSize = zKserver.getVDPsize();
			for(String tbl : tablesList){
				int seqNr = zKclient.getCurrentSeqNr(tbl);
				int totalVDPs = VdpUtils.getTotalVDPs(seqNr, vdpSize);
				//automatically setting the VDPStatus to NOT_MIGRATED
				MigrationStatus status = new MigrationStatus(seqNr, totalVDPs);
				boolean setted = zKserver.setFreshMigrationStatus(tbl, status);
				snapshot.put(tbl, status);
				log.debug(Thread.currentThread().getName()+
						" Added table "+tbl+" to snapshot with seqNr: "+seqNr+" and totalVDPs: "+totalVDPs);
			}
		} catch (Exception e) {
			log.error(Thread.currentThread().getName() +
					" Unable to create a snapshot!", e);
		}
		
		//release the queryLock
		zKserver.unlockQueries();
		zKserver.close();
		log.debug(Thread.currentThread().getName()+
				" unlocked queries...");
	}
	
	/**
	 * Restores the snapshot considering the MigrationStatus stored in ZooKeeper.
	 * @param tablesList The list of tables that will be considered to create the snapshot.
	 * @throws Exception ZK errors, interruptions, etc.
	 */
	protected void restoreSnapshot(List<String> tableList) throws Exception{
		ZKserver zKserver = new ZKserver(connectString);
		zKserver.lockQueries();
		
		for(String tbl : tableList){
			while(!zKserver.acquireLock(tbl)){
				log.error(Thread.currentThread().getName()+
							" Cannot acquire lock on table: "+tbl+". Retrying!");
			}
			MigrationStatus migrationStatus = zKserver.getFreshMigrationStatus(tbl, null);
			//reverting all VDPs which are UNDER_MIGRATION to NOT_MIGRATED
			HashMap<Integer, StateMachine> vdps = migrationStatus.getVDPs();
			Set<Integer> keys = vdps.keySet();
			for(Integer vdpId : keys){
				StateMachine sm = vdps.get(vdpId);
				State currentState = sm.getCurrentState();
				if(currentState.equals(State.UNDER_MIGRATION)){
					migrationStatus.getVDPs().put(vdpId, new StateMachine());
					zKserver.setFreshMigrationStatus(tbl, migrationStatus);
					log.debug(Thread.currentThread().getName()+
							" reverting "+tbl+"/VDP "+vdpId+" to NOT_MIGRATED");
				}
			}
			
			zKserver.releaseLock(tbl);
			snapshot.put(tbl, migrationStatus);
			log.debug(Thread.currentThread().getName()+
					" Added table "+tbl+" to snapshot");
		}
		
		zKserver.unlockQueries();
		zKserver.close();
	}
	
	/**
	 * Tells the SRC if it is allowed to migrate a given VDP for a given table.
	 * @param tableName The name of the table to migrate.
	 * @param VDPid The id of the VDP to be migrated.
	 * @return true if it is possible to migrate the given VDP, false otherwise.
	 * @throws Exception ZooKeeper exception
	 */
	public boolean canMigrate(String tableName, int VDPid) throws Exception{
		ZKserver zKserver = new ZKserver(connectString);
		
		while(!zKserver.acquireLock(tableName)){
			//log.error(Thread.currentThread().getName()+
			//			" Cannot acquire lock on table: "+tableName+". Retrying!");
		}
		//log.info(Thread.currentThread().getName() + 
		//			" Got lock!");
		
		MigrationStatus migStatus = zKserver.getFreshMigrationStatus(tableName, null);
		
		State currentState = migStatus.getVDPstatus(VDPid).getCurrentState();
		if(currentState.equals(State.MIGRATED)){
			log.debug(Thread.currentThread().getName() +
					" migration status already "+currentState.name()+" for VDP: "+tableName+"/"+VDPid);
			snapshot.put(tableName, migStatus);
			zKserver.close();
			zKserver=null;
			return false;
		}else if(currentState.equals(State.SYNC)){
			snapshot.put(tableName, migStatus);
			do{
				log.debug(Thread.currentThread().getName()+
						" Looping until the state changes back to NOT_MIGRATED");
				Thread.sleep(300);
				migStatus = zKserver.getFreshMigrationStatus(tableName, null);
				currentState = migStatus.getVDPstatus(VDPid).getCurrentState();
			}while(currentState.equals(State.SYNC));
			
		} else if(currentState.equals(State.UNDER_MIGRATION)){
			snapshot.put(tableName, migStatus);
			zKserver.releaseLock(tableName);
			zKserver.close();
			zKserver=null;
			return false;
		}
		
		VDPstatus migrateVDP = migStatus.migrateVDP(VDPid);
		int retries = 0;
		while(!zKserver.setFreshMigrationStatus(tableName, migStatus) && retries < 10){
			log.error(Thread.currentThread().getName() + 
					" ==> !!! Couldn't update the migration status to "+migrateVDP.name()+" for VDP: "+tableName+"/"+VDPid);
			Thread.sleep(100);
			retries++;
		}
		snapshot.put(tableName, migStatus);
		
		zKserver.releaseLock(tableName);
		
		zKserver.close();
		zKserver=null;
		log.debug(Thread.currentThread().getName() +
				" updated migration status to "+migStatus.getVDPstatus(VDPid).getCurrentState()+" for VDP: "+tableName+"/"+VDPid);
		return migStatus.getVDPstatus(VDPid).getCurrentState().name().equals(VDPstatus.UNDER_MIGRATION.name()) && retries < 10 ? true : false;
	}
	
	/**
	 * Called from the TWC to announce the complete migration of a given VDP.
	 * @param tableName The table name.
	 * @param VDPid The id of the VDP the TWC has finished to migrate.
	 * @return true if the operation succeeded, false otherwise.
	 * @throws Exception ZooKeeper exception.
	 */
	protected boolean notifyFinishedMigration(String tableName, int VDPid) throws Exception{
		ZKserver zKserver = new ZKserver(connectString);
		
		while(!zKserver.acquireLock(tableName)){
			//log.error(Thread.currentThread().getName()+
			//			" Cannot acquire lock on table: "+tableName+". Retrying!");
		}
		//log.info(Thread.currentThread().getName() + 
		//			" Got lock!");
		
		MigrationStatus migStatus = zKserver.getFreshMigrationStatus(tableName, null);
		VDPstatus migratedVDP = migStatus.finish_migrateVDP(VDPid);
		int retries = 0;
		while(!zKserver.setFreshMigrationStatus(tableName, migStatus) && retries < 10){
			log.error(Thread.currentThread().getName() + 
					" ==> !!! Couldn't update the migration status to "+migratedVDP.name()+" for VDP: "+tableName+"/"+VDPid);
			Thread.sleep(100);
			retries++;
		}
		zKserver.releaseLock(tableName);
		zKserver.close();
		zKserver=null;
		log.debug(Thread.currentThread().getName() +
				" updated migration status to "+migratedVDP.name()+" for VDP: "+tableName+"/"+VDPid);
		return migStatus.getVDPstatus(VDPid).getCurrentState().name().equals(VDPstatus.MIGRATED.name()) && retries < 10 ? true : false;
	}
	
	/**
	 * Returns a list containing all the tables of the database.
	 * @return The list of tables.
	 */
	public abstract List<String> getTableList();
	
	/**
	 * When migrated in a partitioned way, this method MUST be called by the TWC in order to 
	 * report that an entity has been migrated.
	 * The method updates the counters relative to the VDP that contains the entity.
	 * @param myModel The Metamodel entity.
	 */
	public void updateVDPsCounters(Metamodel myModel){
		Integer VDPid = VdpUtils.getVDP(Integer.parseInt(myModel.getRowKey()), vdpSize);
		Map<String, Integer> vdpSizeMap = myModel.getActualVdpSize();
		Set<String> columnFamilies = vdpSizeMap.keySet();
		int size = (int) Math.pow(10, vdpSize);
		for(String cf : columnFamilies){
			int updatedValue = vdpsCounters.putAndIncrementCounter(cf, VDPid, size);
			//log.debug(Thread.currentThread().getName()+
			//		"\n updating VDPs Counters: "
			//		+"piggybacked value: "+vdpSizeMap.get(cf)
			//		+ " set counter "+cf+"/"+VDPid+" to: "+updatedValue);
			
			//check if we finished to migrate an entire VDP
			if(updatedValue>=vdpSizeMap.get(cf)){
				boolean proof = false;
				int retries = 0;
				while(!proof && retries <= 3){
					try {
						proof = notifyFinishedMigration(cf, VDPid);
					} catch (Exception e) {
						log.error("Unable to update MigrationStatus for VDP "+VDPid+
								" in table "+cf, e);
						retries++;
					}
				}
			}
		}
	}
}
