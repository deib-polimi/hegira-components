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
package it.polimi.hegira.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Class containing all the constants used throughout the project. 
 * @author Marco Scavuzzo
 */
public class Constants {
	
	/**
	* LOGS FILE PATH
	*/
	public static final String LOGS_PATH = "log.properties";
	
	/**
	 * QUEUE FILE PATH
	 */
	public static final String QUEUE_PATH = "queue.properties";
	
	/**
	 * CREDENTIALS FILE PATH
	 */
	public static final String CREDENTIALS_PATH = "credentials.properties";
	/**
	 * CASSANDRA CONFIGURATION FILE PATH
	 */
	public static final String CASSANDRA_CONFIGURATION_FILE="cassandraConfiguration.properties";
	
	/**
	* STATUS RESPONSE
	*/
	public static final String STATUS_SUCCESS = "OK";
	public static final String STATUS_ERROR = "ERROR";
	public static final String STATUS_WARNING = "WARNING";
	
	/**
	* SUPPORTED DATABASE IDENTIFIERS
	*/
	public static final String GAE_DATASTORE="DATASTORE";
	public static final String AZURE_TABLES="TABLES";
	public static final String AMAZON_DYNAMODB="DYNAMODB";
	public static final String CASSANDRA="CASSANDRA";
	public static List<String> getSupportedDatabseList(){
		ArrayList<String> list = new ArrayList<String>();
		list.add(GAE_DATASTORE);
		list.add(AZURE_TABLES);
		list.add(AMAZON_DYNAMODB);
		list.add(CASSANDRA);
		return list;
	}
	public static boolean isSupported(String database){
		List<String> supportedList = getSupportedDatabseList();
		return supportedList.contains(database);
	}
	public static List<String> getSupportedDBfromList(List<String> databases){
		List<String> supportedList = getSupportedDatabseList();
		databases.retainAll(supportedList);
		return databases;
	}
	
	/**
	* CREDENTIALS PROPERTIES
	* Properties names stored in CREDENTIALS FILE
	*/
	public static final String AZURE_PROP = "azure.storageConnectionString";
	public static final String DATASTORE_USERNAME = "datastore.username";
	public static final String DATASTORE_PASSWORD = "datastore.password";
	public static final String DATASTORE_SERVER = "datastore.server";
	public static final String CASSANDRA_SERVER = "cassandra.server";
	public static final String CASSANDRA_USERNAME = "cassandra.username";
	public static final String CASSANDRA_PASSWORD = "cassandra.password";
	public static List<String> getSupportedCredentials(){
		ArrayList<String> list = new ArrayList<String>();
		list.add(AZURE_PROP);
		list.add(DATASTORE_USERNAME);
		list.add(DATASTORE_PASSWORD);
		list.add(DATASTORE_SERVER);
		list.add(CASSANDRA_SERVER);
		list.add(CASSANDRA_USERNAME);
		list.add(CASSANDRA_PASSWORD);
		return list;
	}
	public static final String ZK_CONNECTSTRING = "zookeeper.connectString";
	
	/**
	 * CASSANDRA CONFIFURATION PROPERTIES
	 * Properties stored in the CASSANDRA CONFIGURATION file
	 */
	public static final String KEYSPACE = "cassandra.keyspace";
	public static final String READ_CONSISTENCY= "cassandra.readConsistency";
	public static final String PRIMARY_KEY_NAME="cassandra.primarKey";
	/**
	 * CASSANDRA CONSISTENCY LEVELS
	 */
	public static final String CONSISTENCY_EVENTUAL="eventual";
	public static final String CONSISTENCY_STRONG="strong";
	/**
	* PRODUCER AND CONSUMER CONSTANTS
	*/
	public static final String PRODUCER = "producer";
	public static final String CONSUMER = "consumer";
	
	/**
	 * DEFAULT TABLE NAME
	 * (used during the migration of data to Cassandra when the metamodel does NOT specify any column family)
	 */
	public static final String DEFAULT_TABLE_CASSANDRA="defaultTableCassandra";

}