package it.polimi.hegira.adapters.cassandra;

import org.apache.log4j.Logger;

import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.utils.ConfigurationManagerCassandra;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.PropertiesManager;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * This class implements the singleton pattern.
 * In this way all threads are forced to share the same session.
 * This is a recomended practice when dealing with a single keyspace.
 * 
 * @author Andrea Celli
 *
 */
public class SessionManager {

	private static Logger log = Logger.getLogger(SessionManager.class);
	
	//the instance of the manager
	private static SessionManager manager;
	//the session shared among differend threads
	private static Session session;
	
	private SessionManager() throws ConnectException{
		//credentials
		String server=PropertiesManager.getCredentials(Constants.CASSANDRA_SERVER);
		String username=PropertiesManager.getCredentials(Constants.CASSANDRA_USERNAME);
		String password=PropertiesManager.getCredentials(Constants.CASSANDRA_PASSWORD);
		String keyspace=ConfigurationManagerCassandra.getConfigurationProperties(Constants.KEYSPACE);
		//build the session
			try{
				Cluster.Builder clusterBuilder=Cluster.builder()
						.addContactPoint(server)
						.withCredentials(username, password);
				Cluster cluster=clusterBuilder.build();
				this.session=cluster.connect(keyspace);
				return;
			}catch(NoHostAvailableException | 
					AuthenticationException |
					IllegalStateException ex){
				log.error(Thread.currentThread().getName() + " - Not able to connect to Cassandra ",ex);
				throw new ConnectException(ex);
			}
	}
	
	/**
	 * returns the unique instance of the session manager.
	 * creates the instance if it does not already exist.
	 * @return SessionManager
	 * @Throws ConnectException (raised if the session manager is not able to connect to Cassandra)
	 */
	public static SessionManager getSessionManager() throws ConnectException{
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
