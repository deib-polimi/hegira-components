package it.polimi.hegira.adapters.cassandra;

import it.polimi.hegira.models.CassandraModel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
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
	//the current session
	private Session session;
	//flag that signals if the table has been altered
	private boolean changed;
	//number of columns contained in the table
	private int tableSize;
	
	/**
	 * Initialize all the variables and creates the new table in cassandra.
	 * When created the table contains only the column for the id values.
	 * @param tableName
	 */
	public Table(String tableName){
		this.tableName=tableName;
		columns=new ArrayList<String>();
		//retrieves the unique session from the session manager
		session=SessionManager.getSessionManager().getSession();
		changed=false;
		tableSize=0;
		createInitialTable(tableName);
	}
	

	/**
	 * This methods inserts a row in the table.
	 * This methods is synchronized in order to avoid conflicts due to different threads changing the same table.
	 * 
	 * @param row - the row to be insterted
	 */
	public synchronized void insert(CassandraModel row){
		//number of column contained in the row
		int rowSize=row.getColumns().size();
		
	}
	
	/**
	 * This method creates the table in the cassandra db.
	 * the initial table will contain only an id column
	 * @param tableName2
	 */
	private void createInitialTable(String tableName) {
		session.execute(
			      "CREATE TABLE IF NOT EXISTS " + tableName + " ( id varchar PRIMARY KEY );");
	}
}
