package it.polimi.hegira.adapters.cassandra;

import it.polimi.hegira.exceptions.ConnectException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the singleton pattern so that all the threads share the same instance of the Table Manager.
 * The Table Manager manages Tables instances and assign them to threads when needed.
 * 
 * @author Andrea Celli
 *
 */
public class TablesManager {

	//instance of the TableManager 
	private static TablesManager manager;
	//map of the existing tables
	private Map<String,Table> tablesMap;
	
	//private instance of the constructor, SINGLETON PATTER
	private TablesManager(){
		//initialize the tables map
		tablesMap=new HashMap<String, Table>();
	}
	
	/**
	 * Create an instance of the table manager if it does not already exist.
	 * Otherwise returns the existing TableManager.
	 * @return the TableManager
	 */
	public static TablesManager getTablesManager(){
		//create a new TablesManager only if it does not alreay exist
		if(manager==null){
			manager=new TablesManager();
		}
		return manager;
	}
	
	/**
	 * returns true if the TablesManager contain the specified table
	 * @param String - tableName
	 * @return true if the table is already contained in the map, false otherwise
	 */
	public boolean contains(String tableName){
		return tablesMap.containsKey(tableName);
	}
	
	/**
	 * Returns the Table named as specified in the parameter. 
	 * The method checks if the table already exists, if it does not already exist the method creates it.
	 * 
	 * @param tableName
	 * @return Table - the table corresponding to the specified name
	 * @Throws ConnectException (raised if the session manager is not able to connect to Cassandra)
	 */
	public Table getTable(String tableName) throws ConnectException{
		//check if the table exists
		if(!contains(tableName)){
			//creates it if it does not exists
			createTable(tableName);
		}
		return tablesMap.get(tableName);
		
	}

	/**
	 * Creates a new table with the specified name and adds it to the map
	 * @param tableName
	 * @Throws ConnectException (raised if the session manager is not able to connect to Cassandra)
	 */
	private void createTable(String tableName) throws ConnectException {
		tablesMap.put(tableName, new Table(tableName));
	}
	
	
}
