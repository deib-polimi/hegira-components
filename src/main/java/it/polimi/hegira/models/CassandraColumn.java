package it.polimi.hegira.models;
/**
 * 
 * @author Andrea Celli
 *
 */
public class CassandraColumn {
	
	private String columnName;
	//value is deserialized in the Transformer if its type is supported
	//Otherwise it will still be serialized
	private Object columnValue;
	private String valueType;
	private boolean indexed;
	
	
	public CassandraColumn(){
		
	}
	/**
	 * Create a complete Cassandra column
	 * @param columnName
	 * @param columnValue
	 * @param valueType
	 * @param serialized
	 * @param indexed
	 */
	public CassandraColumn(String columnName,Object columnValue,String valueType,boolean indexed){
		this.columnName=columnName;
		this.columnValue=columnValue;
		this.valueType=valueType;
		this.indexed=indexed;
	}
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public Object getColumnValue() {
		return columnValue;
	}
	public void setColumnValue(Object columnValue) {
		this.columnValue = columnValue;
	}
	public String getValueType() {
		return valueType;
	}
	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	public boolean isIndexed() {
		return indexed;
	}
	public void setIndexed(boolean indexed) {
		this.indexed = indexed;
	}	
}
