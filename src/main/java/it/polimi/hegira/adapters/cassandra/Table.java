package it.polimi.hegira.adapters.cassandra;


import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.utils.CassandraTypesUtils;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.PropertiesManager;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.hamcrest.Condition.Step;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.CreateIndex;
import com.datastax.driver.core.schemabuilder.CreateIndex.CreateIndexOn;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

/**
 * This class manages a single Table.
 * Used to check columns, alter the table if necessary, and perform inserts into Cassandra.
 * @author Andrea Celli
 *
 */
public class Table {
	private static Logger log = Logger.getLogger(Table.class);

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
	 * @throws ClassNotFoundException, InvalidParameterException
	 */
	public synchronized void insert(CassandraModel row) throws ClassNotFoundException,InvalidParameterException{
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
				alterTable(name,colsToInsert.get(i-1).getValueType(),colsToInsert.get(i-1).isIndexed());
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
		String columnsNames=initColumnList(tableName);
		defaultPrepared=createPreparedStatement(columnsNames,columns.size()-1);
	}
	
	/**
	 *  initialize the list of columns
	 *	it's needed to manage a table that already exists in the database
	 * @param tableName
	 * @return string of names of the initial columns
	 */
	private String initColumnList(String tableName){
		String keyspace=PropertiesManager.getCredentials(Constants.CASSANDRA_KEYSPACE);
		//string of column names to build the first prepared statement
		String columnNames=" ";
		List<ColumnMetadata> existingColumns=session
				.getCluster()
				.getMetadata()
				.getKeyspace(keyspace)
				.getTable(tableName)
				.getColumns();
		
		for(int i=0;i<existingColumns.size()-1;i++){
				String name=existingColumns.get(i).getName();
				columns.add(name);
				columnNames=columnNames+name+", ";
		}
		//last element
		String name=existingColumns.get(existingColumns.size()-1).getName();
		columns.add(name);
		columnNames=columnNames+name;
		
		return columnNames;
	}

	/**
	 * Returns a new prepared statement to insert a new row with the given columns.
	 * @param columnNames
	 * @param rowSize - number of columns contained in the row (without id)
	 * @return PreparedStatement
	 */
	private PreparedStatement createPreparedStatement(
			String columnNames,int rowSize) {
		String completeStatementString=packString(columnNames,rowSize);
		return session.prepare(completeStatementString);
	}
	
	/**
	 * Takes as input the string containing column names in the format:
	 * name1, name2, name3...
	 * Pack the string with the remaining parts in order to use it to build a preparedStatement to perform an insert.
	 * The final string will be in the form:
	 * "INSERT INTO <tableName> (<name1>, <name2>, <name3>) VALUES (?,?,?)"
	 * @param columnNames
	 * @param rowSize - number of columns contained in the row (without id)
	 * @return the complete string for a prepared statement that performs an insert
	 */
	private String packString(String columnNames,int rowSize) {
		String completeString="INSERT INTO "+tableName+" ( "+columnNames+" ) VALUES ( ";
		//there's at least a ? for the id
		String questionMarks="?";
		//add other values question marks for remaining columns
		for(int i=0;i<rowSize;i++){
			questionMarks=questionMarks+",?";
		}
		completeString=completeString+questionMarks+" ) ";
		return completeString;
	}


	/**
	 * Returns a prepared statement and eventually updates the default prepared statement.
	 * 1) table has been altered-->create and set a new default statement, return the default statement
	 * 2) table has not been altered
	 *  2.1)same number of columns-->return the default PreparedStatement (reuse to increase efficiency)
	 *  2.2)lower number of columns-->compute and return a new prepared statement (without setting it as default)
	 * @param columnNames
	 * @param rowSize
	 * @return PreparedStatement
	 */
	private PreparedStatement getPreparedStatement(String columnNames,
			int rowSize) {
		//table has been changed
		if(isChanged()){
			defaultPrepared=createPreparedStatement(columnNames, rowSize);
			return defaultPrepared;
		}else{
			//not changed and same number of columns
			//+1 takes into account the id
			if(rowSize+1==columns.size()){
				return defaultPrepared;
			}else{
				//lower number of columns but no table changes
				return createPreparedStatement(columnNames, rowSize);
			}
		}
	}
	
	/**
	 * This method performs an alter on the table in order to introduce a new column.
	 * 
	 * @param name - name of the new column
	 * @param valueType - the string representing the type of the new column
	 * @param indexed - true if the column has to be indexed
	 */
	private void alterTable(String name, String valueType, boolean indexed) throws ClassNotFoundException,InvalidParameterException{
		SchemaStatement alter;
		try{
			//build the alter
			alter=SchemaBuilder
					.alterTable(tableName)
					.addColumn(name)
					.type(CassandraTypesUtils.getCQLDataType(valueType));
			//execute the alter
			session.execute(alter);
			
			//create a new index if needed
			if(indexed){
				SchemaStatement createIndex=SchemaBuilder
						.createIndex(name+"_index")
						.onTable(tableName)
						.andColumn(name);
				
				session.execute(createIndex);
			}
			
			//add the column to the columns list
			columns.add(name);
			
			//TODO: altre catch
		}catch(ClassNotFoundException | InvalidParameterException ex){
			log.error("Error while altering table: "+tableName+"\nStackTrace:\n"+ex.getStackTrace());
			ex.printStackTrace();
			throw  ex;
		}
	}


	public String getTableName() {
		return tableName;
	}

	public List<String> getColumns() {
		return columns;
	}

	public Session getSession() {
		return session;
	}

	private boolean isChanged() {
		return changed;
	}


	private void setChanged(boolean changed) {
		this.changed = changed;
	}
}
