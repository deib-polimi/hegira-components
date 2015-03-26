package it.polimi.hegira.transformers;

import java.io.IOException;
import java.util.ArrayList;
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
	
		//Column families are not mapped since Cassandra does not support them
		mapRowKey(metamodel,model);
		mapColumns(metamodel,model);
		mapPartitionGroup(metamodel,model);
		
		return metamodel;
	}

	@Override
	public CassandraModel fromMyModel(Metamodel model) {
		// TODO Auto-generated method stub
		return null;
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
	

}
