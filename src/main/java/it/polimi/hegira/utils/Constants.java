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
	public static List<String> getSupportedDatabseList(){
		ArrayList<String> list = new ArrayList<String>();
		list.add(GAE_DATASTORE);
		list.add(AZURE_TABLES);
		list.add(AMAZON_DYNAMODB);
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
	public static List<String> getSupportedCredentials(){
		ArrayList<String> list = new ArrayList<String>();
		list.add(AZURE_PROP);
		list.add(DATASTORE_USERNAME);
		list.add(DATASTORE_PASSWORD);
		list.add(DATASTORE_SERVER);
		return list;
	}
	public static final String ZK_CONNECTSTRING = "zookeeper.connectString";
	
	/**
	* PRODUCER AND CONSUMER CONSTANTS
	*/
	public static final String PRODUCER = "producer";
	public static final String CONSUMER = "consumer";
	
	/**
	 * TYPES OF CONSISTENCY	
	 */
	public static final String EVENTUAL="eventual";
	public static final String STRONG="strong";

}