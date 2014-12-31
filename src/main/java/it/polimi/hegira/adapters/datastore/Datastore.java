package it.polimi.hegira.adapters.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entities;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.google.appengine.api.datastore.Entity;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.DatastoreModel;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.transformers.DatastoreTransformer;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.PropertiesManager;

public class Datastore extends AbstractDatabase {
	private static Logger log = Logger.getLogger(Datastore.class);
	//private RemoteApiInstaller installer;
	//private DatastoreService ds;
	
	private class ConnectionObject{
		public ConnectionObject(){}
		public ConnectionObject(RemoteApiInstaller installer, DatastoreService ds){
			this.installer = installer;
			this.ds = ds;
		}
		protected RemoteApiInstaller installer;
		protected DatastoreService ds;
	}
	
	public Datastore(Map<String, String> options) {
		super(options);
		if(THREADS_NO>0){
			connectionList = new ArrayList<ConnectionObject>(THREADS_NO);
			for(int i=0;i<THREADS_NO;i++)
				connectionList.add(new ConnectionObject());
		}else{
			connectionList = new ArrayList<ConnectionObject>(1);
			connectionList.add(new ConnectionObject());
		}
	}

	private ArrayList<ConnectionObject> connectionList;
	
	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		// TWC
		//log.debug(Thread.currentThread().getName()+" Hi I'm the GAE consumer!");
		List<Entity> batch = new ArrayList<Entity>();
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		long k = 0;
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		while(true){
			log.debug(Thread.currentThread().getName()+" Extracting from the taskQueue"+thread_id+" THREADS_NO: "+THREADS_NO);
			
			try {
				Delivery delivery = taskQueues.get(thread_id).getConsumer().nextDelivery(2000);
				if(delivery!=null){
					Metamodel myModel = new Metamodel();
					deserializer.deserialize(myModel, delivery.getBody());
					
					DatastoreTransformer dt = new DatastoreTransformer(connectionList.get(thread_id).ds);
					DatastoreModel fromMyModel = dt.fromMyModel(myModel);
					
					batch.add(fromMyModel.getEntity());
					batch.add(fromMyModel.getFictitiousEntity());
					
					taskQueues.get(thread_id).sendAck(delivery.getEnvelope().getDeliveryTag());
					k++;
					
					if(k%100==0){
						putBatch(batch);
						log.debug(Thread.currentThread().getName()+" ===>100 entities. putting normal batch");
						batch = new ArrayList<Entity>();
					}else{
						if(k>0){
							//log.debug(Thread.currentThread().getName()+" ===>Nothing in the queue for me!");
							putBatch(batch);
							log.debug(Thread.currentThread().getName()+" ===>less than 100 entities. putting short batch");
							batch = new ArrayList<Entity>();
							k=0;
						}
					}
				}
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException e) {
				log.error("Error consuming from the queue "+TaskQueue.getDefaultTaskQueueName(),
						e);
			} catch (TException e) {
				log.error("Errore deserializing", e);
			} catch (QueueException e) {
				log.error("Couldn't send the ack to the queue "+TaskQueue.getDefaultTaskQueueName(),
						e);
			}
		}
	}

	@Override
	protected Metamodel toMyModel(AbstractDatabase db) {
		//TODO: implement VDPs retrieval
		
		Datastore datastore = (Datastore) db;
		List<String> kinds = datastore.getAllKinds();
		int thread_id = 0;
		
		for(String kind : kinds){
			long i=0, previousQueueCheckTime=0;
			int queueElements=0;
			
			//Create a new instance of the Thrift Serializer
	        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
	        //Datastore cursor to scan a kind
	        Cursor cursor = null;
	        
	        while(true){
	        		QueryResultList<Entity> results = datastore.getEntitiesByKind_withCursor(kind, cursor, 300);
	        		
	        		//CURSOR CODE
	        		Cursor newcursor = getNextCursor(results);
				/**
				 * newcursor is null if the query result cannot be resumed;
				 * newcursor is equal to cursor if all entities have been read.
				 */
				if(newcursor==null || newcursor.equals(cursor)) break;
				else cursor = newcursor;
				
				//PRODUCTION CODE
				for(Entity entity : results){
					DatastoreModel dsModel = new DatastoreModel(entity);
					dsModel.setAncestorString(entity.getKey().toString());
					DatastoreTransformer dt = new DatastoreTransformer();
					Metamodel myModel = dt.toMyModel(dsModel);
					
					if(myModel!=null){
						try {
							taskQueues.get(thread_id).publish(serializer.serialize(myModel));
							i++;
						} catch (QueueException | TException e) {
							log.error("Serialization Error: ", e);
						}
					}
				}
				log.debug(Thread.currentThread().getName()+" Produced: "+i+" entities");
				
				if(i%5000==0)
					taskQueues.get(0).slowDownProduction();
	        }
	        log.debug(Thread.currentThread().getName()+" ==> Transferred "+i+" entities of kind "+kind);
		}
		return null;
	}
	
	/**
	 * Returns the next cursor relative to the given list of entities.
	 * @param results The list of entities.
	 * @return	The next cursor. <code>null</code> if the query result cannot be resumed;
	 */
	private Cursor getNextCursor(QueryResultList<Entity> results){
		//trying to minimize undocumented errors from the Datastore
		boolean proofCursor = true;
		int timeout_ms=100, retries=10;
		
		Cursor newcursor=null;
		while(proofCursor && retries>0){
			try{
				newcursor = results.getCursor();
				proofCursor = false;
			}catch(Exception e){
				log.error("\n\n\n\n\t\tUndocumented Error !!! "+e.getMessage()+"\n\n\n");
				try {
					Thread.sleep(timeout_ms);
					if(timeout_ms<5000) timeout_ms*=2;
				} catch (InterruptedException e1){
					proofCursor=false;
				}
				retries--;
			}
		}
		return newcursor;
	}

	@Override
	public void connect() throws ConnectException {
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		if(!isConnected()){
			String username = PropertiesManager.getCredentials(Constants.DATASTORE_USERNAME);
			String password = PropertiesManager.getCredentials(Constants.DATASTORE_PASSWORD);
			String server = PropertiesManager.getCredentials(Constants.DATASTORE_SERVER);
			RemoteApiOptions options = new RemoteApiOptions()
        		.server(server, 443)
        		.credentials(username, password);
			try {
				log.debug(Thread.currentThread().getName()+" - Logging into "+server);
				RemoteApiInstaller installer = new RemoteApiInstaller();
				installer.install(options);
				DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
				ConnectionObject co = new ConnectionObject(installer, ds);
				connectionList.add(thread_id,co);
				log.debug(Thread.currentThread().getName()+" - Added connection object at "+
				"position: "+connectionList.indexOf(co)+
				" ThreadId%THREAD_NO="+thread_id);
			} catch (IOException e) {
				log.error(DefaultErrors.connectionError+"\nStackTrace:\n"+e.getStackTrace());
				throw new ConnectException(DefaultErrors.connectionError);
			}
		}else{
			log.warn(DefaultErrors.alreadyConnected);
			throw new ConnectException(DefaultErrors.alreadyConnected);
		}
	}

	/**
	 * Checks if a connection has already been established
	 * @return true if connected, false if not.
	 */
	public boolean isConnected(){
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		try{
			return (connectionList.get(thread_id).installer==null || 
					connectionList.get(thread_id).ds==null) ? false : true;
		}catch(IndexOutOfBoundsException e){
			return false;
		}
	}
	
	@Override
	public void disconnect() {
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		if(isConnected()){
			if(connectionList.get(thread_id).installer!=null)
				connectionList.get(thread_id).installer.uninstall();
			connectionList.get(thread_id).installer = null;
			connectionList.get(thread_id).ds = null;
			log.debug(Thread.currentThread().getName() + " Disconnected");
		}else{
			log.warn(DefaultErrors.notConnected);
		}
	}
	
	/**
	 * Stores a List of {@link com.google.appengine.api.datastore.Entity} in batch
	 * @param batch
	 */
	private void putBatch(List<Entity> batch){
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		boolean proof = true;
		while(proof){
			try{
				connectionList.get(thread_id).ds.put(batch);
				proof = false;
			}catch(ConcurrentModificationException ex){
				log.error(ex.getMessage()+"...retry");
			}
		}
	}
	
	/**
    * Query for a given entity type
    * @param ds The datastore object to connect to the actual datastore
    * @param kind The kind used for the retrieval
    * @return An iterable containing all the entities
    */
   private Iterable<Entity> getEntitiesByKind(String kind){
	   int thread_id = 0;
	   if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
   		Query q = new Query(kind);
   		PreparedQuery pq = connectionList.get(thread_id).ds.prepare(q);
   		return pq.asIterable();
   }
   
   /**
    * Gets a batch of entities of a given kind
    * @param kind The Entity Kind 
    * @param cursor The point where to start fetching entities (<code>null</code> if entities should be fetched). Could be extracted from the returned object.
    * @param pageSize The number of entities to be retrieved in each batch (maximum 300).
    * @return An object containing the entities, the cursor and other stuff.
    */
   private QueryResultList<Entity> getEntitiesByKind_withCursor(String kind, 
   		Cursor cursor, int pageSize){
	   
	   int thread_id = 0;
	   if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
   		boolean proof = true;
   		QueryResultList<Entity> results = null;
	   	/**
	   	 * Bullet proof reads from the Datastore.
	   	 */
	   	while(proof){
	   		try{
		    		FetchOptions fetchOptions = FetchOptions.Builder.withLimit(pageSize);
		    		if(cursor!=null)
		    			fetchOptions.startCursor(cursor);
		        	Query q = new Query(kind);
		        	PreparedQuery pq = connectionList.get(thread_id).ds.prepare(q);
		        	results = pq.asQueryResultList(fetchOptions);
		        	proof = false;
	   		}catch(Exception e){
	   			log.error(Thread.currentThread().getName() + 
	   					"ERROR: getEntitiesByKind_withCursor -> "+e.getMessage());
	   		}
	       	
	   	}
			
	   	return results;
   }
   
   /**
    * Gets all the entities descending (even not directly connected) from the given key
    * @param ds the Datastore object
    * @param ancestorKey get descendents of this key
    * @return
    */
   private Iterable<Entity> getDescentents(Key ancestorKey){
	   int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
	    	Query q = new Query().setAncestor(ancestorKey);
	    	PreparedQuery pq = connectionList.get(thread_id).ds.prepare(q);
	    	return pq.asIterable();
   }
   
   /**
    * All entities' kinds contained in the datastore, excluding statistic ones.
    * @return  A list containing all the kinds
    */
   public List<String> getAllKinds(){
	   int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
	   	Iterable<Entity> results = connectionList.get(thread_id).ds.prepare(new Query(Entities.KIND_METADATA_KIND)).asIterable();
	   	//list containing kinds of the root entities
	   	ArrayList<String> kinds = new ArrayList<String>();
	   	for(Entity globalStat : results){
	   		Key key2 = globalStat.getKey();
	   		String name = key2.getName();
		    	if(name.indexOf("_")!=0){
		    		kinds.add(name);
		    	}
	   	}
	   	return kinds;
   }
   
   /**
    * Checks if an entity is root (i.e. it hasn't any parent)
    * @param e The entity to be checked
    * @return <code>true</code> if the given entity is root, <code>false</code> otherwise;
    */
   private boolean isRoot(Entity e){
	   	Key key = e.getKey();
	   	Key parentKey = key.getParent();
	   	return (parentKey==null) ? true : false;
   }
}
