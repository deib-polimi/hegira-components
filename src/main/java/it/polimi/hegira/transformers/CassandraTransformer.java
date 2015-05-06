package it.polimi.hegira.transformers;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.event.EventUtils;
import org.apache.log4j.Logger;

import it.polimi.hegira.adapters.cassandra.Cassandra;
import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.utils.CassandraTypesUtils;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultSerializer;

/**
 * 
 * @author Andrea Celli
 *
 */
public class CassandraTransformer implements ITransformer<CassandraModel> {
	
	//this variable is used to determine wheter the consistency has to be strong or eventual
	private String consistency;
	
	public CassandraTransformer(){
		
	}
	
	/**
	 * Create a new transformer setting the consistency level that has to be used
	 * 
	 * @param consistency
	 * @throws IllegalArgumentException when the parameter is not a supported level of consistency
	 */
	public CassandraTransformer(String consistency) throws IllegalArgumentException{
	  setConsistency(consistency);
	}
	
	@Override
	public Metamodel toMyModel(CassandraModel model) {
		Metamodel metamodel=new Metamodel();
	
		mapColumnsFamilies(metamodel,model);
		mapRowKey(metamodel,model);
		mapColumns(metamodel,model);
		mapPartitionGroup(metamodel,model);
		
		return metamodel;
	}

	@Override
	public CassandraModel fromMyModel(Metamodel model) {
		CassandraModel cassandraModel=new CassandraModel();
		
		mapKey(cassandraModel,model);
		mapTable(cassandraModel,model);
		mapColumns(cassandraModel,model);
		
		return cassandraModel;
	}

	public String getConsistency() {
		return consistency;
	}

	/**
	 * Set the consistency level that has to be used
	 * 
	 * @param consistency
	 * @throws IllegalArgumentException  when the parameter is not a supported level of consistency
	 */
	public void setConsistency(String consistency) throws IllegalArgumentException{
		  if(consistency.equals(Constants.CONSISTENCY_EVENTUAL) || consistency.equals(Constants.CONSISTENCY_STRONG)){
				this.consistency=consistency;
		  }else
				throw new IllegalArgumentException("consistency level not supported");
	}
	
	/*-----------------------------------------------------------------*/
	/*----------------DIRECT MAPPING UTILITY METHODS-------------------*/
	/*-----------------------------------------------------------------*/

	/**
	 * Only one column family is created. The table in which the row is contained determines its value.
	 * 
	 * @param metamodel
	 * @param model
	 */
	private void mapColumnsFamilies(Metamodel metamodel, CassandraModel model) {
		List<String> columnFamilies=new ArrayList<String>();
		String tableName=model.getTable();
		columnFamilies.add(tableName);
		metamodel.setColumnFamilies(columnFamilies);
	}

	/**
	 * Sets partition group in the metamodel depending on the chosen level of consistency
	 * 
	 * @param metamodel
	 * @param model
	 */
	private void mapPartitionGroup(Metamodel metamodel, CassandraModel model) {
		if (consistency.equals(Constants.CONSISTENCY_EVENTUAL)){
			metamodel.setPartitionGroup("@"+model.getTable()+"#"+model.getKeyValue());
		}else
			if(consistency.equals(Constants.CONSISTENCY_STRONG))
				metamodel.setPartitionGroup("@strong#strong");
	}

	/**
	 * Map cassandra row's cells (columns) to metamodel's columns
	 * @param metamodel
	 * @param model
	 */
	private void mapColumns(Metamodel metamodel, CassandraModel model) {
		
		List<Column> allColumns=new ArrayList<Column>();
		
		for(CassandraColumn cassandraColumn:model.getColumns()){
			//check if the column is empty
			if(cassandraColumn.getColumnValue()!=null){
				Column metaModColumn=new Column();
				
				try{
				metaModColumn.setColumnValue(DefaultSerializer.serialize(cassandraColumn.getColumnValue()));
				}catch(IOException ex){
					ex.printStackTrace();
				}
				
				metaModColumn.setColumnName(cassandraColumn.getColumnName());
				metaModColumn.setIndexable(cassandraColumn.isIndexed());
				metaModColumn.setColumnValueType(cassandraColumn.getValueType());
				
				allColumns.add(metaModColumn);
			}
		}
		
		//since Cassandra does not have column families I use the name of the table as the key for all the columns
		metamodel.getColumns().put(model.getTable(), allColumns);	
	}

	/**
	 * Maps the Cassandra primary key value to the entity key in the metamodel
	 * @param metamodel
	 * @param model
	 */
	private void mapRowKey(Metamodel metamodel, CassandraModel model) {
		metamodel.setRowKey(model.getKeyValue());
	}
	
	
	/*-----------------------------------------------------------------*/
	/*----------------INVERSE MAPPING UTILITY METHODS-------------------*/
	/*-----------------------------------------------------------------*/	
	
	/**
	 *This method sets the table name taking it from the FIRST element of the list containing column families in the
	 *meta-model.
	 *When there are no column Families available (the metamodel contains an empty list of column families) the system 
	 *assigns a default name to the table.
	 * 
	 * @param cassandraModel
	 * @param model
	 */
	private void mapTable(CassandraModel cassandraModel, Metamodel model) {
		List<String> columnFamilies=model.getColumnFamilies();
		if(columnFamilies.size()>0){
			cassandraModel.setTable(columnFamilies.get(0));
		}else{
			cassandraModel.setTable(Constants.DEFAULT_TABLE_CASSANDRA);
		}
	}

	/**
	 * This method maps the entity key to the row key
	 * 
	 * @param cassandraModel
	 * @param model
	 */
	private void mapKey(CassandraModel cassandraModel, Metamodel model) {
		cassandraModel.setKeyValue(model.getRowKey());
	}
	
	/**
	 * This method maps properties of the metamodel into Cassandra columns.
	 * Before deserializing the value it checks if the data type is supported in Cassandra. 
	 * If it is supported the value is deserialized and the type converted to one of the types supported by Cassandra.
	 * If it is NOT supported the value is kept serialized. In this case a new column with the following structure
	 * is added: name: <Column_name>"_Type", value: <the original data type>
	 * 
	 * @param cassandraModel
	 * @param model
	 */
	private void mapColumns(CassandraModel cassandraModel, Metamodel model) {
		Iterator<String> columnFamilyIterator=model.getColumnFamiliesIterator();
		
		while(columnFamilyIterator.hasNext()){
			String columnFamily=columnFamilyIterator.next();
			//get the properties contained in the actual column family
			List<Column> columnsMeta=model.getColumns().get(columnFamily);
			if(columnsMeta!=null){
			for(Column column:columnsMeta){
				CassandraColumn cassandraColumn=new CassandraColumn();
				
				cassandraColumn.setColumnName(column.getColumnName());
				cassandraColumn.setIndexed(column.isIndexable());
				
				String javaType=column.getColumnValueType();
				try{
					if(!CassandraTypesUtils.isSupportedCollection(javaType)){
							setValueAndSimpleType(cassandraColumn,column.getColumnValue(),javaType);
					}else{
						String collectioType=CassandraTypesUtils.getCollectionType(javaType);
						if(collectioType.equals("Map")){
							String type1=CassandraTypesUtils.getFirstSimpleType(javaType);
							String type2=CassandraTypesUtils.getSecondSimpleType(javaType);
							CassandraTypesUtils.checkIfSupported(type1);
							CassandraTypesUtils.checkIfSupported(type2);
						}else{
							String subType=javaType.substring(javaType.indexOf("<")+1,javaType.indexOf(">"));
							CassandraTypesUtils.checkIfSupported(subType);
						}
					    deserializeCollection(cassandraColumn,column.getColumnValue(),javaType);
					    CassandraTypesUtils.translateCollectionType(cassandraColumn, javaType);
				}
			 }catch(ClassNotFoundException e){
				 //leave the value serialized and wrap it in a ByteBuffer
				 cassandraColumn.setColumnValue(ByteBuffer.wrap(column.getColumnValue()));
				 cassandraColumn.setValueType("blob");
				 //create and add to the row a column containing the type NOT supported
				 cassandraModel.addColumn(createTypeColumn(column)); 
			 }catch(IOException e){
				 e.printStackTrace();
			 }
				cassandraModel.addColumn(cassandraColumn);
			}
		}
		}
	}


	/**
	 * Depending on the value of the type the method:
	 * 1) deserialize the value and assign it to the new cassandra column
	 * 2) update the cassandra type to a CQL supported type
	 * 
	 * @param cassandraColumn
	 * @param columnValue the value of the columns in the metamodel
	 * @param javaType 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void setValueAndSimpleType(CassandraColumn cassandraColumn,
			byte[] columnValue, String javaType) throws IOException,ClassNotFoundException {
		switch(javaType){
			case "String":
				cassandraColumn.setColumnValue((String) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("varchar");
				return;
			case "Long":
				cassandraColumn.setColumnValue((Long) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("bigint");
				return;
			case "byte[]":
				cassandraColumn.setColumnValue(ByteBuffer.wrap(columnValue));
				cassandraColumn.setValueType("blob");
				return;
			case "Boolean":
				cassandraColumn.setColumnValue((Boolean) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("boolean");
				return;	
			case "BigDecimal":
				cassandraColumn.setColumnValue((BigDecimal) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("decimal");
				return;
			case "Double":
				cassandraColumn.setColumnValue((double) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("double");
				return;
			case "Float":
				cassandraColumn.setColumnValue((float) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("float");
				return;
			case "InetAddress":
				cassandraColumn.setColumnValue((InetAddress) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("inet");
				return;
			case "Integer":
				cassandraColumn.setColumnValue((int) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("int");
				return;
			case "Date":
				cassandraColumn.setColumnValue((Date) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("timestamp");
				return;
			case "UUID":
				cassandraColumn.setColumnValue((UUID) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("uuid");
				return;
			case "BigInteger":
				cassandraColumn.setColumnValue((BigInteger) DefaultSerializer.deserialize(columnValue));
				cassandraColumn.setValueType("varint");
				return;
			default: 
				throw  new ClassNotFoundException();
		}
		
	}

	/**
	 * Deserialize the collection type
	 * @param cassandraColumn
	 * @param columnValue
	 * @param javaType
	 * @throws IOException
	 */
	private void deserializeCollection(CassandraColumn cassandraColumn,
			byte[] columnValue, String javaType) throws IOException,ClassNotFoundException{
		String collectionType=CassandraTypesUtils.getCollectionType(javaType);
		if(collectionType.equals("Map")){
			Map<?,?> map=(Map<?, ?>) DefaultSerializer.deserialize(columnValue);
			cassandraColumn.setColumnValue(map);
		}else{
			if(collectionType.equals("List")){
				List<?> list=(List<?>) DefaultSerializer.deserialize(columnValue);
				cassandraColumn.setColumnValue(list);
			}else{
				if(collectionType.equals("Set")){
					Set<?> set=(Set<?>) DefaultSerializer.deserialize(columnValue);
					cassandraColumn.setColumnValue(set);
				}
			}
		}
	}
	
	/**
	 * Returns a new cassandra column containing the info about a not supported type
	 * format of the name: typeNotSupp_Type
	 * @param column
	 * @return CassandraColumn
	 */
	private CassandraColumn createTypeColumn(Column column) {
		return new CassandraColumn(column.getColumnName()+"_Type", column.getColumnValueType(), "varchar", false);
	}
	
}
