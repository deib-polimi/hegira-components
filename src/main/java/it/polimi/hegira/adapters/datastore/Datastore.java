package it.polimi.hegira.adapters.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

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

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.PropertiesManager;

public class Datastore extends AbstractDatabase {
	private static Logger log = Logger.getLogger(Datastore.class);
	private RemoteApiInstaller installer;
	private DatastoreService ds;
	
	public Datastore(Map<String, String> options) {
		super(options);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Metamodel toMyModel(AbstractDatabase model) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void connect() throws ConnectException {
		if(!isConnected()){
			String username = PropertiesManager.getCredentials(Constants.DATASTORE_USERNAME);
			String password = PropertiesManager.getCredentials(Constants.DATASTORE_PASSWORD);
			String server = PropertiesManager.getCredentials(Constants.DATASTORE_SERVER);
			RemoteApiOptions options = new RemoteApiOptions()
        		.server(server, 443)
        		.credentials(username, password);
			try {
				log.debug("Logging into "+server);
				installer = new RemoteApiInstaller();
				installer.install(options);
				ds = DatastoreServiceFactory.getDatastoreService();
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
		return (installer==null || ds==null) ? false : true;
	}
	
	@Override
	public void disconnect() {
		if(isConnected()){
			if(installer!=null)
				installer.uninstall();
			installer = null;
			ds = null;
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
		boolean proof = true;
		while(proof){
			try{
				ds.put(batch);
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
   		Query q = new Query(kind);
   		PreparedQuery pq = ds.prepare(q);
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
		        	PreparedQuery pq = ds.prepare(q);
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
	    	Query q = new Query().setAncestor(ancestorKey);
	    	PreparedQuery pq = ds.prepare(q);
	    	return pq.asIterable();
   }
   
   /**
    * All entities' kinds contained in the datastore, excluding statistic ones.
    * @return  A list containing all the kinds
    */
   public List<String> getAllKinds(){
	   	Iterable<Entity> results = ds.prepare(new Query(Entities.KIND_METADATA_KIND)).asIterable();
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
