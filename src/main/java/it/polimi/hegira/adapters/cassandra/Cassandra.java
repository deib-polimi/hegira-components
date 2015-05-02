package it.polimi.hegira.adapters.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.adapters.datastore.Datastore;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.transformers.CassandraTransformer;
import it.polimi.hegira.utils.CassandraTypesUtils;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.PropertiesManager;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.Row;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
/**
 * 
 * @author Andrea Celli
 *
 */
public class Cassandra extends AbstractDatabase {
	private static Logger log = Logger.getLogger(Cassandra.class);
	
	private List<ConnectionObject> connectionList;
	
	private class ConnectionObject{
		protected Session session;
		public ConnectionObject(){}
		public ConnectionObject(Session session){
			this.session=session;
		}
	}
	
	/**
	 * The constructor creates a ConnectionObject for each
	 * thread and adds it to the connectionList
	 * @param options
	 */
	public Cassandra(Map<String, String> options){
		super(options);
		if(THREADS_NO>0){
			connectionList = Collections.synchronizedList(new  ArrayList<ConnectionObject>(THREADS_NO));
			//the creation of empty objects is needed to execute the method isConnected()
			for(int i=0;i<THREADS_NO;i++)
				connectionList.add(new ConnectionObject());
		}else{
			connectionList = Collections.synchronizedList(new ArrayList<ConnectionObject>(1));
			connectionList.add(new ConnectionObject());
		}
	}
	
	@Override
	public void connect() throws ConnectException {
		int thread_id=0;
		if(THREADS_NO!=0)
			thread_id=(int) (Thread.currentThread().getId()%THREADS_NO);
		
		if(!isConnected()){
			
			try{
				log.debug(Thread.currentThread().getName()+" - Logging into server");
				//
				//retrieves the unique session from the session manager
				//
				Session session=SessionManager.getSessionManager().getSession();
				ConnectionObject conObj= new ConnectionObject(session);
				//I use set in order to keep things in order with the empty connectioObjects
				connectionList.set(thread_id, conObj);
				
				log.debug(Thread.currentThread().getName()+" - Added connection object at "+
					"position: "+connectionList.indexOf(conObj)+
					" ThreadId%THREAD_NO="+thread_id);
		}catch(NoHostAvailableException | AuthenticationException | IllegalStateException ex){
			log.error(DefaultErrors.connectionError+"\nStackTrace:\n"+ex.getStackTrace());
			throw new ConnectException(DefaultErrors.connectionError);
		}
	}else{
		log.warn(DefaultErrors.alreadyConnected);
		throw new ConnectException(DefaultErrors.alreadyConnected);
	}	
	}
	
	/**
	 * Checks if a connection has already been established for the current 
	 * thread
	 * @return true if connected, false if not.
	 */
	public boolean isConnected(){
		int thread_id=0;
		if(THREADS_NO!=0)
			thread_id=(int) (Thread.currentThread().getId()%THREADS_NO);
		try{
			return (connectionList.get(thread_id).session==null) ? false : true;
		}catch(IndexOutOfBoundsException e){
			return false;
		}
	}
	
	@Override
	public void disconnect() {
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		if(isConnected()){
			connectionList.get(thread_id).session.close();
			connectionList.get(thread_id).session = null;
			log.debug(Thread.currentThread().getName() + " Disconnected");
		}else
			log.warn(DefaultErrors.notConnected);	
	}

	
	@Override
	protected Metamodel toMyModel(AbstractDatabase model) {
		int thread_id = 0;
		if(THREADS_NO!=0)
			thread_id = (int) (Thread.currentThread().getId()%THREADS_NO);
		
		//Create a new instance of the Thrift Serializer
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        //Create a new instance of the cassandra transformer used to translate entities into the
        //metamodel format
        /**
         * TODO: manage the consistency type
         */
        CassandraTransformer cassandraTransformer=new CassandraTransformer(Constants.EVENTUAL_CONSISTENCY);
		
		Session session=connectionList.get(thread_id).session;
		//
		//TODO: aggiungere try/catch sulla lista di tables
		//
		// get the list of all tables contained in the keyspace
		Cluster cluster=session.getCluster();
		String keySpace=PropertiesManager.getCredentials(Constants.CASSANDRA_KEYSPACE);
		Collection<TableMetadata> tables=cluster
				.getMetadata()
				.getKeyspace(keySpace)
				.getTables();
		
		for(TableMetadata table:tables){
			//get the name of the table
			String tableName=table.getName();

			try{
				//QUERY all the rows in the actual table
				ResultSet queryResults=session.execute("SELECT * FROM "+tableName);
				//
				//transformation to the metamodel for each row
				//
				for(Row row : queryResults){
					//create a new cassandra model instance
					CassandraModel cassModel = new CassandraModel();
					//set the table
					cassModel.setTable(tableName);
					//set the key
					//the primary key name has to be set by default (see limitations)
					String key=row.getString(Constants.DEFAULT_PRIMARY_KEY_NAME);
					cassModel.setKeyValue(key);
					//
					//COLUMNS
					//
					Iterator<ColumnDefinitions.Definition> columnsIterator=row.getColumnDefinitions().iterator();
					while(columnsIterator.hasNext()){
						ColumnDefinitions.Definition column=columnsIterator.next();
						
						//don't add the id column
						if(!column.getName().equals("id")){
						CassandraColumn cassColumn=new CassandraColumn();
						
						//set name
						String columnName=column.getName();
						cassColumn.setColumnName(columnName);
						try{
							//set type + value 
							String dataType=column.getType().toString();
							setValueAndType(cassColumn,columnName,dataType,row);
							//set indexed 
							if(table.getColumn(columnName).getIndex()!=null){
								cassColumn.setIndexed(true);
							}else{
								cassColumn.setIndexed(false);
							}
							//add to the cassandra model
							cassModel.addColumn(cassColumn);
							
						}catch(ClassNotFoundException ex){
							log.error(Thread.currentThread().getName() + " - Error managing column: "+column.getName()
									+" for row: "+key
									+" on cassandra table: "+tableName, ex);
						}
				    }
				}
					//from cassandraModel to MetaModel
					Metamodel meta=cassandraTransformer.toMyModel(cassModel);
					List<Column> colz=meta.getColumns().get("players");
					try{
						//serialize & add to the queue
						taskQueues.get(thread_id).publish(serializer.serialize(meta));
					}catch (QueueException e) {
						log.error(Thread.currentThread().getName() + " - Error communicating with the queue " + 
								TaskQueue.getDefaultTaskQueueName(), e);
					} catch (TException e) {
						log.error(Thread.currentThread().getName() + " - Error serializing message ", e);
					}
			  }
			}catch(NoHostAvailableException | 
					QueryExecutionException |
					QueryValidationException | 
					UnsupportedFeatureException ex){
				log.error(Thread.currentThread().getName() + " - Error during the query on cassandra table: "+tableName, ex);
			}
			
		}
		
		return null;
	}
	

	/**
	 * This method set the type of the cassandra column and, according to the specific type, it retrieve and sets the value
	 * @param cassandraColumn
	 * @param columnName
	 * @param dataType -  CQL data type
	 * @param row
	 * @throws TypeNotPresentException
	 */
	private void setValueAndType(CassandraColumn cassandraColumn, String columnName,String dataType, Row row) throws ClassNotFoundException{
		
		if(!CassandraTypesUtils.isCollection(dataType)){
			setValueAndSimpleType(cassandraColumn,  columnName, dataType, row);
		}else{
			setValueAndCollectionType(cassandraColumn, columnName, dataType, row);
		}
		
	}

	/**
	 * 1) simple CQL type --> java type & set the type in cassandra model column
	 * 2) retrieve the value on the base of the java type
	 * @param cassandraColumn
	 * @param columnName
	 * @param dataType - string containing the CQL type
	 * @param row
	 * @throws ClassNotFoundException
	 */
	private void setValueAndSimpleType(CassandraColumn cassandraColumn,
			String columnName, String dataType, Row row) throws ClassNotFoundException{
		switch(dataType){
		case "ascii": 
			cassandraColumn.setValueType("String");
			cassandraColumn.setColumnValue(row.getString(columnName));
			return;
		case "bigint":
			cassandraColumn.setValueType("Long");
			cassandraColumn.setColumnValue(row.getLong(columnName));
			return;
		case "blob":
			cassandraColumn.setValueType("byte[]");
			//TODO: check
			cassandraColumn.setColumnValue(row.getBytes(columnName));
			return;
		case "boolean":
			cassandraColumn.setValueType("Boolean");
			cassandraColumn.setColumnValue(row.getBool(columnName));
			return;
		case "counter":
			cassandraColumn.setValueType("Long");
			cassandraColumn.setColumnValue(row.getLong(columnName));
			return;
		case "decimal":
			cassandraColumn.setValueType("BigDecimal");
			cassandraColumn.setColumnValue(row.getDecimal(columnName));
			return;
		case "double":
			cassandraColumn.setValueType("Double");
			cassandraColumn.setColumnValue(row.getDouble(columnName));
			return;
		case "float":
			cassandraColumn.setValueType("Float");
			cassandraColumn.setColumnValue(row.getFloat(columnName));
			return;
		case "inet":
			cassandraColumn.setValueType("InetAddress");
			cassandraColumn.setColumnValue(row.getInet(columnName));
			return;
		case "int":
			cassandraColumn.setValueType("Integer");
			cassandraColumn.setColumnValue(row.getInt(columnName));
			return;
		case "text":
			cassandraColumn.setValueType("String");
			cassandraColumn.setColumnValue(row.getString(columnName));
			return;
		case "timestamp":
			cassandraColumn.setValueType("Date");
			cassandraColumn.setColumnValue(row.getDate(columnName));
			return;
		case "uuid":
			cassandraColumn.setValueType("UUID");
			cassandraColumn.setColumnValue(row.getUUID(columnName));
			return;
		case "varchar":
			cassandraColumn.setValueType("String");
			cassandraColumn.setColumnValue(row.getString(columnName));
			return;
		case "varint":
			cassandraColumn.setValueType("BigInteger");
			cassandraColumn.setColumnValue(row.getVarint(columnName));
			return;
		case "timeuuid":
			cassandraColumn.setValueType("UUID");
			cassandraColumn.setColumnValue(row.getUUID(columnName));
			return;
		case "udt":
			cassandraColumn.setValueType("UDTValue");
			cassandraColumn.setColumnValue(row.getUDTValue(columnName));
			return;
		case "tuple":
			cassandraColumn.setValueType("TupleValue");
			cassandraColumn.setColumnValue(row.getTupleValue(columnName));
			return;
		case "custom":
			//TODO: check
			//TODO: check docs, I said custom types were not supported
			cassandraColumn.setValueType("ByteBuffer");
			cassandraColumn.setColumnValue(row.getBytes(columnName));
			return;
		default: 
			throw  new ClassNotFoundException();
		}
		
	}
	
	/**
	 * Manage the conversion of a CQL collection type to java type
     * 1) simple CQL type --> java type & set the type in cassandra model column
	 * 2) retrieve the value on the base of the java type
	 * 
	 * @param cassandraColumn
	 * @param columnName
	 * @param dataType - collection type in the form X<Y, Z> or X<Y>
	 * @param row
	 * @throws ClassNotFoundException
	 */
	private void setValueAndCollectionType(CassandraColumn cassandraColumn,
			String columnName,String dataType, Row row) throws ClassNotFoundException{
		
		//check if the collection is one of the supported types
		CassandraTypesUtils.isSupportedCollection(dataType);
		
		String collectionType=CassandraTypesUtils.getCollectionType(dataType);
		
		if(collectionType.equals("map")){
			String CQLSubType1=CassandraTypesUtils.getFirstSimpleType(dataType);
			//retrieve the second subtype removing spaces in the string
			String CQLSubType2=CassandraTypesUtils.getSecondSimpleType(dataType).replaceAll("\\s","");
			//Set the column type
			cassandraColumn.setValueType("Map<"+CassandraTypesUtils.getJavaSimpleType(CQLSubType1)+","+
					CassandraTypesUtils.getJavaSimpleType(CQLSubType2)+">");
			//set the column value
			cassandraColumn.setColumnValue(row.getMap(columnName, Object.class, Object.class));
		}else{
			//the collection has only one subtype
			//retrieve the subtype
			String CQLSubType=dataType.substring(dataType.indexOf("<")+1,dataType.indexOf(">"));
			if(collectionType.equals("set")){
				//set type
				cassandraColumn.setValueType("Set<"+CassandraTypesUtils.getJavaSimpleType(CQLSubType)+">");
				//set the value
				cassandraColumn.setColumnValue(row.getSet(columnName, Object.class));
			}else{
				if(collectionType.equals("list")){
					//set type
					cassandraColumn.setValueType("List<"+CassandraTypesUtils.getJavaSimpleType(CQLSubType)+">");
					//set the value
					cassandraColumn.setColumnValue(row.getList(columnName, Object.class));
				}
			}
		}
		
	}

	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		log.debug(Thread.currentThread().getName()+" Cassandra consumer started ");
		
		//Thrift Deserializer
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		//retrieve thread number
		int thread_id=0;
		if(THREADS_NO!=0){
			thread_id=(int) (Thread.currentThread().getId()%THREADS_NO);
		}
		//instantiate the cassandra transformer
		//the consistency level is not needed. Entity inserted with eventual consistency
		CassandraTransformer transformer=new CassandraTransformer();
		//instantiate the TableManager
		TablesManager tablesManager=TablesManager.getTablesManager();
		
		while(true){
			log.debug(Thread.currentThread().getName()+" Extracting from the taskQueue"+thread_id+
					" THREADS_NO: "+THREADS_NO);
			try{
				//extract from the task queue
				Delivery delivery=taskQueues.get(thread_id).getConsumer().nextDelivery();
				if(delivery!=null){
					//deserialize and retrieve the metamodel
					Metamodel metaModel=new Metamodel();
					deserializer.deserialize(metaModel, delivery.getBody());
					//retrieve the Cassandra Model
					CassandraModel cassandraModel=transformer.fromMyModel(metaModel);
					
					taskQueues.get(thread_id).sendAck(delivery);
					
					//retrieve the table and perform the insert
					tablesManager.getTable(cassandraModel.getTable()).insert(cassandraModel);
				}else{
					log.debug(Thread.currentThread().getName() + " - The queue " +
							TaskQueue.getDefaultTaskQueueName() + " is empty");
					return null;
				}
			}catch(ShutdownSignalException |
					ConsumerCancelledException |
					InterruptedException ex){
				log.error(Thread.currentThread().getName() + " - Cannot read next delivery from the queue " + 
						TaskQueue.getDefaultTaskQueueName(), ex);
			}catch(TException ex){
				log.error(Thread.currentThread().getName() + " - Error deserializing message ", ex);
			}catch(QueueException ex){
				log.error(Thread.currentThread().getName() + " - Error sending an acknowledgment to the queue " + 
						TaskQueue.getDefaultTaskQueueName(), ex);
			}catch(ClassNotFoundException ex){
				log.error(Thread.currentThread().getName() + " - Error in during the insertion -", ex);
			}
			
		}
	}

	@Override
	protected AbstractDatabase fromMyModelPartitioned(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Metamodel toMyModelPartitioned(AbstractDatabase model) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getTableList() {
		// TODO Auto-generated method stub
		return null;
	}

}
