package it.polimi.hegira.adapters.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.adapters.datastore.Datastore;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.PropertiesManager;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
/**
 * 
 * @author Andrea Celli
 *
 */
public class Cassandra extends AbstractDatabase {
	private static Logger log = Logger.getLogger(Cassandra.class);
	
	private List<ConnectionObject> connectionList;
	
	private class ConnectionObject{
		protected Session session;
		public ConnectionObject(){}
		public ConnectionObject(Session session){
			this.session=session;
		}
	}
	
	/**
	 * The constructor creates a ConnectionObject for each
	 * thread and adds it to the connectionList
	 * @param options
	 */
	public Cassandra(Map<String, String> options){
		super(options);
		if(THREADS_NO>0){
			connectionList = Collections.synchronizedList(new  ArrayList<ConnectionObject>(THREADS_NO));
			//the creation of empty objects is needed to execute the method isConnected()
			for(int i=0;i<THREADS_NO;i++)
				connectionList.add(new ConnectionObject());
		}else{
			connectionList = Collections.synchronizedList(new ArrayList<ConnectionObject>(1));
			connectionList.add(new ConnectionObject());
		}
	}
	
	@Override
	public void connect() throws ConnectException {
		int thread_id=0;
		if(THREADS_NO!=0)
			thread_id=(int) (Thread.currentThread().getId()%THREADS_NO);
		
		if(!isConnected()){
			String server=PropertiesManager.getCredentials(Constants.CASSANDRA_SERVER);
			String username=PropertiesManager.getCredentials(Constants.CASSANDRA_USERNAME);
			String password=PropertiesManager.getCredentials(Constants.CASSANDRA_PASSWORD);
			String keyspace=PropertiesManager.getCredentials(Constants.CASSANDRA_KEYSPACE);
			
			try{
				log.debug(Thread.currentThread().getName()+" - Logging into "+server);
				Cluster.Builder clusterBuilder=Cluster.builder()
						.addContactPoint(server)
						.withCredentials(username, password);
				Cluster cluster=clusterBuilder.build();
				Session session=cluster.connect(keyspace);
				
				ConnectionObject conObj= new ConnectionObject(session);
				//I use set in order to keep things in order with the empty connectioObjects
				connectionList.set(thread_id, conObj);
				
				log.debug(Thread.currentThread().getName()+" - Added connection object at "+
					"position: "+connectionList.indexOf(conObj)+
					" ThreadId%THREAD_NO="+thread_id);
		}catch(NoHostAvailableException | AuthenticationException | IllegalStateException ex){
			log.error(DefaultErrors.connectionError+"\nStackTrace:\n"+ex.getStackTrace());
			throw new ConnectException(DefaultErrors.connectionError);
		}
	}else{
		log.warn(DefaultErrors.alreadyConnected);
		throw new ConnectException(DefaultErrors.alreadyConnected);
	}	
	}
	
	/**
	 * Checks if a connection has already been established for the current 
	 * thread
	 * @return true if connected, false if not.
	 */
	public boolean isConnected(){
		int thread_id=0;
		if(THREADS_NO!=0)
			thread_id=(int) (Thread.currentThread().getId()%THREADS_NO);
		try{
			return (connectionList.get(thread_id).session==null) ? false : true;
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
			connectionList.get(thread_id).session.close();
			connectionList.get(thread_id).session = null;
			log.debug(Thread.currentThread().getName() + " Disconnected");
		}else
			log.warn(DefaultErrors.notConnected);	
	}

	
	@Override
	protected Metamodel toMyModel(AbstractDatabase model) {
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		
		Session session=connectionList.get(thread_id).session;
		
		// get the list of all tables contained in the keyspace
		Cluster cluster=session.getCluster();
		List<TableMetadata> tables=(ArrayList<TableMetadata>) cluster
				.getMetadata()
				.getKeyspace(Constants.CASSANDRA_KEYSPACE)
				.getTables();
		
		for(TableMetadata table:tables){
			//get the name of the table
			String tableName=table.getName();
			
			//QUERY all the rows in the actual table
			ResultSet queryResults=session.execute("SELECT * FROM "+tableName);
			
			//do the transformation to the metamodel for each row
			
		}
		
		return null;
	}
	
	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected AbstractDatabase fromMyModelPartitioned(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Metamodel toMyModelPartitioned(AbstractDatabase model) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getTableList() {
		// TODO Auto-generated method stub
		return null;
	}

}
