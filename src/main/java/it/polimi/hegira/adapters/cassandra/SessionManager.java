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
 * This is a recomended practice when dealing with a single keyspace.)
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
	
	/*
	 * version 2
	private String server;
	private String username;
	private String password;
	private String keyspace;*/
	
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
	 * 
	 * @return session
	 */
	public Session getSession() {//throws ConnectException{
		/*
		 *version 2 
		 *
		Session session;
		try{
			Cluster.Builder clusterBuilder=Cluster.builder()
					.addContactPoint(server)
					.withCredentials(username, password);
			Cluster cluster=clusterBuilder.build();
			session=cluster.connect(keyspace);
			log.debug("new session created for thread: "+Thread.currentThread().getName());
			return session;
		}catch(NoHostAvailableException | 
				AuthenticationException |
				IllegalStateException ex){
			log.error(Thread.currentThread().getName() + " - Not able to connect to Cassandra ",ex);
			throw new ConnectException(ex);
	}*/
	return session;
 }
}
