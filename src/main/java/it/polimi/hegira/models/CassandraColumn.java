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
