package it.polimi.hegira.adapters.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * This class manages a single Table.
 * Used to check columns, alter the table if necessary, and perform inserts into Cassandra.
 * @author Andrea Celli
 *
 */
public class Table {

	private String tableName;
	private List<String> columns;
	
	public Table(String tableName){
		this.tableName=tableName;
		columns=new ArrayList<String>();
	}
	
	/**
	 * This methods inserts a row in the table.
	 * This methods is synchronized in order to avoid conflicts due to different threads changing the same table.
	 * 
	 * @param row - the row to be insterted
	 * @param session - the session of the current thread
	 */
	public synchronized void insert(Row row,Session session){
		
	}
}
