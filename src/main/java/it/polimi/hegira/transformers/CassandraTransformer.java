package it.polimi.hegira.transformers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.event.EventUtils;

import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.Metamodel;
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
		  if(consistency==Constants.EVENTUAL || consistency==Constants.STRONG){
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
		if (consistency==Constants.EVENTUAL){
			metamodel.setPartitionGroup("@"+model.getTable()+"#"+model.getKeyValue());
		}else
			if(consistency==Constants.STRONG)
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
			for(Column column:columnsMeta){
				CassandraColumn cassandraColumn=new CassandraColumn();
				
				cassandraColumn.setColumnName(column.getColumnName());
				cassandraColumn.setIndexed(column.isIndexable());
				
				String javaType=column.getColumnValueType();
				/*if(!isSupportedCollection(javaType)){
					if(isSupported(javaType)){
						//Deserializza e aggiorna tipo
						//TODO
					}else{
						//crea nuova colonna per il tipo e lascia serializzato
						//TODO
					}
				}else{
					String type1=getFirstSimpleType(javaType);
					String type2=getSecondSimpleType(javaType);
					if(isSupported(type1) && isSupported(type2)){
						//Deserializza e aggiorna tipo
						//TODO
					}else{
						//crea nuova colonna per il tipo e lascia serializzato
						//TODO
					}
				}*/
				try{
					if(!isSupportedCollection(javaType)){
							//Deserializza e aggiorna tipo
							//TODO
					}else{
						String type1=getFirstSimpleType(javaType);
						String type2=getSecondSimpleType(javaType);
						checkIfSupported(type1);
						checkIfSupported(type2);
							//Deserializza e aggiorna tipo
							//TODO
				}
			 }catch(ClassNotFoundException e){
				  //crea classe per il tipo non supportato e lascia serializzato il valore
			 }
		  }
		}
	}

	/**
	 * @param type
	 * @return true if the type is one of the collections supported by Cassandra (map,list,set)
	 */
	private boolean isSupportedCollection(String type) throws ClassNotFoundException{
		//I suppose the supported types are in the form Map<T,K>,List<T>,Set<T>
		if(type.contains("<") && type.contains(">")){
			String collectionType=type.substring(0, type.indexOf("<"));
			if(collectionType=="Map"||collectionType=="List" || collectionType=="Set"){
				return true;
			}else
				throw new ClassNotFoundException();
		}
		return false;
	}
	/**
	 * TODO
	 */
	private String getFirstSimpleType(String completeType){
		return null;
	}
	/**
	 * TODO
	 */
	private String getSecondSimpleType(String completeType){
		return null;
	}
	/**
	 * TODO
	 */
	private void checkIfSupported(String type) throws ClassNotFoundException{
	}
	
		
}
