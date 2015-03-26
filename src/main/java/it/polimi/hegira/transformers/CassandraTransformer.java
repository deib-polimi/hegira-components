package it.polimi.hegira.transformers;

import org.apache.commons.lang3.event.EventUtils;

import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.utils.Constants;

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

	private void mapColumns(Metamodel metamodel, CassandraModel model) {
		// TODO Auto-generated method stub
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
