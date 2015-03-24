package it.polimi.hegira.transformers;

import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Metamodel;

/**
 * 
 * @author Andrea Celli
 *
 */
public class CassandraTransformer implements ITransformer<CassandraModel> {

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

	/*-----------------------------------------------------------------*/
	/*----------------DIRECT MAPPING UTILITY METHODS-------------------*/
	/*-----------------------------------------------------------------*/

	
	private void mapPartitionGroup(Metamodel metamodel, CassandraModel model) {
		// TODO Auto-generated method stub
		
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
