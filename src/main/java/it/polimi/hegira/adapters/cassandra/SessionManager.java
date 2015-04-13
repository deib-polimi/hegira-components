package it.polimi.hegira.adapters.cassandra;

import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.PropertiesManager;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * This class implements the singleton pattern.
 * In this way all threads are forced to share the same session.
 * This is a recomended practice when dealing with a single keyspace.
 * 
 * @author Andrea Celli
 *
 */
public class SessionManager {

	//the instance of the manager
	private static SessionManager manager;
	//the session shared among differend threads
	private static Session session;
	
	private SessionManager(){
		//credentials
		String server=PropertiesManager.getCredentials(Constants.CASSANDRA_SERVER);
		String username=PropertiesManager.getCredentials(Constants.CASSANDRA_USERNAME);
		String password=PropertiesManager.getCredentials(Constants.CASSANDRA_PASSWORD);
		String keyspace=PropertiesManager.getCredentials(Constants.CASSANDRA_KEYSPACE);
		//build the session
		Cluster.Builder clusterBuilder=Cluster.builder()
				.addContactPoint(server)
				.withCredentials(username, password);
		Cluster cluster=clusterBuilder.build();
		this.session=cluster.connect(keyspace);
	}
	
	/**
	 * returns the unique instance of the session manager.
	 * creates the instance if it does not already exist.
	 * @return
	 */
	public static SessionManager getSessionManager(){
		if(manager==null){
			manager=new SessionManager();
		}
		return manager;
	}
	
	/**
	 * get the unique session
	 * @return session
	 */
	public Session getSession(){
		return session;
	}
}
