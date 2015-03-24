package it.polimi.hegira.models;

import java.util.ArrayList;
import java.util.List;


/**
 * 
 * 
 * @author Andrea Celli
 *
 */
public class CassandraModel {
	private String table;
	//The value of the pre-defined primary key column
	private String keyValue;
	private List<CassandraColumn> columns;
	
	public CassandraModel(){
		initColumns();
	}
	/**
	 * Constructs the model for a given pair table/row key
	 * @param table the table to which the row will belong    
	 * @param keyValue  the row key
	 */
	public CassandraModel(String table,String keyValue){
		this.table=table;
		this.keyValue=keyValue;
		initColumns();
	}
	
	/**
	 * Add a new column to the Cassandra row
	 * @param column
	 */
	public void addColumn(CassandraColumn column){
		columns.add(column);
	}
	

	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getKeyValue() {
		return keyValue;
	}
	public void setKeyValue(String keyValue) {
		this.keyValue = keyValue;
	}
	public List<CassandraColumn> getColumns() {
		return columns;
	}
	public void setColumns(List<CassandraColumn> columns) {
		this.columns = columns;
	}
	private void initColumns(){
		columns=new ArrayList<CassandraColumn>();
	}	
}

