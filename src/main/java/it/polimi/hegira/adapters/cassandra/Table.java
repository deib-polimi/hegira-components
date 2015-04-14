package it.polimi.hegira.adapters.cassandra;

import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.utils.Constants;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hamcrest.Condition.Step;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
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
	//the prepared statement for the actual table configuration (with all its columns)
	private PreparedStatement defaultPrepared;
	
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
		setChanged(false);
		createInitialTable(tableName);
	}
	

	/**
	 * This methods inserts a row in the table.
	 * This methods is synchronized in order to avoid conflicts due to different threads changing the same table.
	 * 
	 * @param row - the row to be insterted
	 */
	public synchronized void insert(CassandraModel row){
		List<CassandraColumn> colsToInsert=row.getColumns();
		
		//number of column contained in the row
		int rowSize=colsToInsert.size();
		
		//the string used to build the prepared statement
		//it contains columns names and gets progressively update
		//the String will have the format: name1, name2, name3, name4....
		String statementString;
		
		//the array of objects to be inserted into the db
		//the size of the array is rowSize+1 because we have to include the key
		Object[] values=new Object[rowSize+1];
		
		//set the primary key
		statementString="id";
		values[0]=row.getKeyValue();
		
		//CHECK id the table contains the other columns
		//at the same time build the names string and the object array
		for(int i=1;i<rowSize+1;i++){
			
			//update names string
			String name=colsToInsert.get(i-1).getColumnName();
			statementString=statementString + ", "+name;
			//update objects array
			values[i]=colsToInsert.get(i-1).getColumnValue();
			
			//check if the table already contains the column
			if(!columns.contains(name)){
				//THE TABLE NEEDS TO BE UPDATED
				alterTable(name,colsToInsert.get(i-1).getValueType());
				setChanged(true);
			}
		}
		
		//retrieve the statement to be executed
		PreparedStatement execStatement=getPreparedStatement(statementString,rowSize);
		//bing the statement with the array of objects that have to be inserted
		BoundStatement bind=execStatement.bind(values);
		//insert
		session.execute(bind);
	}
	

	/**
	 * This method creates the table in the cassandra db.
	 * the initial table will contain only an id column
	 * @param tableName2
	 */
	private void createInitialTable(String tableName) {
		session.execute(
			      "CREATE TABLE IF NOT EXISTS " + tableName + " ( " + Constants.DEFAULT_PRIMARY_KEY_NAME + " varchar PRIMARY KEY );");
		//update the list of column names contained in the table
		columns.add(Constants.DEFAULT_PRIMARY_KEY_NAME);
		defaultPrepared=createPreparedStatement(Constants.DEFAULT_PRIMARY_KEY_NAME);
	}


	private PreparedStatement createPreparedStatement(
			String defaultPrimaryKeyName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	private PreparedStatement getPreparedStatement(String statementString,
			int rowSize) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * This method performs an alter on the table in order to introduce a new column.
	 * 
	 * @param name
	 * @param valueType
	 */
	private void alterTable(String name, String valueType) {
		// TODO Auto-generated method stub
		
	}


	private boolean isChanged() {
		return changed;
	}


	private void setChanged(boolean changed) {
		this.changed = changed;
	}
}
