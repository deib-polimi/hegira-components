package it.polimi.hegira.transformers;

import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.DatastoreModel;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.DefaultSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Query.FilterPredicate;

public class DatastoreTransformer implements ITransformer<DatastoreModel> {
	private static Logger log = Logger.getLogger(DatastoreTransformer.class);

	private DatastoreService ds;
	/**
	 * Instantiates a DatastoreTransformer object used to map data to and from a Metamodel.
	 */
	public DatastoreTransformer(){
		
	}
	/**
	 * Instantiates a DatastoreTransformer object used to map data to and from a Metamodel.
	 * Passing a DatastoreService will make it possible, for the transformer, to execute queries. 
	 * @param ds A DatastoreService instance
	 */
	public DatastoreTransformer(DatastoreService ds){
		this.ds = ds;
	}
	
	@Override
	public Metamodel toMyModel(DatastoreModel model) {
		Metamodel metamodel = new Metamodel();
		/**OLD IMPLEMENTATION**/
//		Iterable<Entity> ancestorPath = model.getAncestorPath();
//		Entity e = model.getEntity(); 
//		Entity root = ancestorPath.iterator().next();
//		String kind = e.getKind();
//		if(kind.indexOf("@")==0) return null;
//		String key = generateKey(e);
//		//partition group is always generated by the root entity
//		mapPartitionGroup(metamodel, root.getKind(), generateKey(root));
//		mapRowKey(metamodel, key);
//		metamodel.addColumnFamily(kind);
//		mapColumns(metamodel, e, kind);
		
		/**SECOND METHOD **/
		Entity e = model.getEntity();
		String kind = e.getKind();
		if(kind.indexOf("@")==0) return null;
		String key = generateKey(e);
		String rootKind = getRootKind(e.getKey().toString());
		String rootKey = getRootKey(e.getKey().toString());
		//partition group is always generated by the root entity
		mapPartitionGroup(metamodel, rootKind, rootKey);
		mapRowKey(metamodel, key);
		//metamodel.addColumnFamily(kind);
		//TODO: this is the new implementation
		metamodel.getColumnFamilies().add(kind);
		mapColumns(metamodel, e, kind);
		/**************************************/
		
//		int i=0;
//		for(Entity e : ancestorPath){
//			String kind = e.getKind();
//			String key = generateKey(e);
//			
//			if(i==0){
//				mapPartitionGroup(metamodel, kind, key);
//			}
//			if(kind.indexOf("@")!=0){
//				mapRowKey(metamodel, key);
//				metamodel.addColumnFamily(kind);
//				mapColumns(metamodel, e, kind);
//			}else{
//				metamodel.addColumnFamily(kind.substring(1));
//			}
//			
//			i++;
//		}
		return metamodel;
	}

	@Override
	public DatastoreModel fromMyModel(Metamodel metamodel) {
		DatastoreModel model = new DatastoreModel();
		List<String> columnFamilies = metamodel.getColumnFamilies();
		String partitionGroup = metamodel.getPartitionGroup();
		//"metamodels" having the same partitionGroup name should be mapped to the same root entity
		//in order to preserve consistency
		if(isConnected()){
			//Check to see if a fictitious Root Entity already exists
			
			//TODO: this is a test. writing the fictitious entity anyway
			/*Entity fictitiousEntity = getFictitiousEntity(partitionGroup);
			if(fictitiousEntity==null){*/
				Entity fictitiousEntity = createFictitiousEntity(partitionGroup);
				//TODO: test performance
				model.setFictitiousEntity(fictitiousEntity);
			//}
			Key parent_key = fictitiousEntity.getKey();
			log.debug(Thread.currentThread().getName()+" Fictitious Entity created: "+parent_key.getName());
			//Extracting column families
			for(String columnFamily : columnFamilies){
				String rowKey = metamodel.getRowKey();
				Entity entity;
				if(rowKey!=null){
					entity = new Entity(columnFamily, rowKey, parent_key);
					log.debug(Thread.currentThread().getName()+" New Root-Entity created: "+rowKey);
				}else{
					entity = new Entity(columnFamily, parent_key);
					log.debug(Thread.currentThread().getName()+" New NON Root-Entity created: "+rowKey);
				}
				//Since we are going row by row ...
				//Extracting columns and mapping
				//Estraggo le colonne, ma ogni column family ha colonne diverse.
				//faccio in modo di passare una stringa col nome della columnfamily
				//List<Column> columns = metamodel.getColumnsByColumnFamily(columnFamily);
				//TODO: this is the new implementation
				List<Column> columns = metamodel.getColumns().get(columnFamily);
				//log.debug(Thread.currentThread().getName()+" Extracting columns");
				for(Column column : columns){
					mapColumnToEntity(column, entity);
				}
				model.setEntity(entity);
			}
		}else{
			log.error(DefaultErrors.notConnected);
		}
				
		return model;
	}

	private boolean isConnected(){
		return (ds==null)?false:true;
	}
	/*---------------------------------------------------------------------------------*/
	/*------------------------- DIRECT MAPPING UTILITY METHODS ------------------------*/
	
	/**SECOND METHOD!**/
	private String getRootKind(String ancestors){
    	int endIndex = ancestors.indexOf("(");
    	return ancestors.substring(0, endIndex);
    }
    
    private String getRootKey(String ancestors){
    	int startIndex = ancestors.indexOf("(");
    	int endIndex = ancestors.indexOf(")");
    	String substring = ancestors.substring(startIndex+1, endIndex);
    	return substring.replace("\"", "");	
    }
	/**************************************/
	
	private void mapPartitionGroup(Metamodel metamodel, String kind, String key){
		if(kind.indexOf("@")!=0)
			metamodel.setPartitionGroup("@"+kind+"#"+key);
		else
			metamodel.setPartitionGroup(kind+"#"+key);
	}
	private String generateKey(Entity e){
		Key keyObj = e.getKey();
		String key = null;
		if(keyObj.getName()!=null){
			key=keyObj.getName();
		}else if(keyObj.getId()!=0){
			key=""+keyObj.getId();
		}
		return key;
	}
	
	private void mapRowKey(Metamodel metamodel, String key){
		metamodel.setRowKey(key);
	}
	
	private void mapColumns(Metamodel metamodel, Entity e, String kind){
		ArrayList<Column> columns = new ArrayList<Column>();
		Map<String, Object> properties = e.getProperties();
		Set<String> prop_keys = properties.keySet();
		for(String prop_key : prop_keys){
			if(properties.get(prop_key)!=null){
				try {
					Column column = new Column();
					column.setColumnName(prop_key);
					Object object = properties.get(prop_key);
					String type = properties.get(prop_key).getClass().getSimpleName();
					column.setColumnValueType(type);
					if(type.equals("Text")){
						object = object.toString();
					}
					column.setColumnValue(DefaultSerializer.serialize(object));
					boolean unindexedProperty = e.isUnindexedProperty(prop_key);
					
					if(unindexedProperty){
						column.setIndexable(false);
					}else{
						column.setIndexable(true);
					}
					columns.add(column);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		//metamodel.setColumnsByColumnFamily(kind, columns);
		//TODO: this is the new implementation
		metamodel.getColumns().put(kind, columns);
	}
	/*---------------------------------------------------------------------------------*/
	
	
	/*---------------------------------------------------------------------------------*/
	/*------------------------- INVERSE MAPPING UTILITY METHODS ------------------------*/
	/**
	 * Gets a fictitious entity given the <code>partitionGroup</code>
	 * @param partitionGroup A String containing the partitionGroup name
	 * 
	 * Should be called after having verified that the DatastoreService variable has been instantiated (isConnected())
	 */
	private Entity getFictitiousEntity(String partitionGroup){
		int lastIndexOf = partitionGroup.lastIndexOf("#");
		int lastIndexOf2 = partitionGroup.lastIndexOf("@");
		if(lastIndexOf > -1 && lastIndexOf2 > -1){
			String kind = partitionGroup.substring(0, lastIndexOf);
			String keyName = partitionGroup.substring(lastIndexOf+1);
			Query q = new Query();
			log.debug(Thread.currentThread().getName()+" Querying for a Key with name: "+keyName+" and Kind: "+kind);
			FilterPredicate filter = new Query.FilterPredicate(Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.EQUAL, 
					KeyFactory.createKey(kind, keyName));
			q.setFilter(filter);
			PreparedQuery preparedQuery = ds.prepare(q);
			Entity asSingleEntity = preparedQuery.asSingleEntity();
			if(asSingleEntity==null)
				log.debug(Thread.currentThread().getName()+" ==> Returning null fictitious entity");
			else
				log.debug(Thread.currentThread().getName()+" ==> Returning fictitious entity with key: "+asSingleEntity.getKey().getName());
			return asSingleEntity;
		}else{
			log.error("Wrong partitionGroup");
			return null;
		}
	}
	/**
	 * Creates a fictitious root entity and stores it in the datastore
	 * @param partitionGroup A String containing the partitionGroup name
	 * @return The entity that has just been created
	 * 
	 * Should be called after having verified that the DatastoreService variable has been instantiated (isConnected())
	 */
	private Entity createFictitiousEntity(String partitionGroup){
		int lastIndexOf = partitionGroup.lastIndexOf("#");
		int lastIndexOf2 = partitionGroup.lastIndexOf("@");
		if(lastIndexOf > -1 && lastIndexOf2 > -1){
			String kind = partitionGroup.substring(0, lastIndexOf);
			String keyName = partitionGroup.substring(lastIndexOf+1);
			Entity entity = new Entity(kind, keyName);
			//entity.setProperty("isRoot", true);
			//TODO: era stato rimosso per prova performance. Viene inserito in batch da Datastore.java
			//ds.put(entity);
			return entity;
		}else{
			log.error("Wrong partitionGroup");
			return null;
		}
	}
	/**
	 * Maps a column to an entityProperty
	 * @param column The column to be mapped
	 * @param entity The entity that will contain the property
	 */
	private void mapColumnToEntity(Column column, Entity entity){
		String columnName = column.getColumnName();
		byte[] columnValue = column.getColumnValue();
		//column.getColumnValueType();
		boolean indexable = column.isIndexable();
		try {
			Object deser_obj = DefaultSerializer.deserialize(columnValue);
			//TODO: columnValue.instanceOf(tutti i tipi di GAE tranne liste che rimangono serializzate)
			/**
			 * If a String is greater than 500 character 
			 * it must be stored as a Text inside the Datastore
			 */
			if(column.getColumnValueType().equals("String")){
				int deser_length = ((String) deser_obj).length();
				if(deser_length > 500){
					Text deser_text = new Text((String) deser_obj);
					deser_obj = deser_text;
				}
			}
			if(indexable){
				entity.setProperty(columnName, deser_obj);
			}else{
				entity.setUnindexedProperty(columnName, deser_obj);
			}
			//log.debug(Thread.currentThread().getName()+" Added property: "+columnName);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*---------------------------------------------------------------------------------*/
}
