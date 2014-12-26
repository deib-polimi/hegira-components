package it.polimi.hegira.adapters.tables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.ResultContinuation;
import com.microsoft.windowsazure.services.core.storage.ResultSegment;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTable;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.microsoft.windowsazure.services.table.client.TableResult;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.exceptions.TablesReadException;
import it.polimi.hegira.models.AzureTablesModel;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.transformers.AzureTablesTransformer;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.PropertiesManager;

public class Tables extends AbstractDatabase {
	private transient Logger log = Logger.getLogger(Tables.class);
	private CloudStorageAccount account;
	private CloudTableClient tableClient;
	
	public Tables(Map<String, String> options) {
		super(options);
		if(options.get("threads")!=null)
			this.THREADS_NO = Integer.parseInt(options.get("threads"));
	}

	long count = 1;
	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		//TWC
		log.debug(Thread.currentThread().getName()+" Hi I'm the AZURE consumer!");
		//Instantiate the Thrift Deserializer
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		
		while(true){
			try {
				Delivery delivery = taskQueue.getConsumer().nextDelivery();
				if(delivery!=null){
					Metamodel myModel = new Metamodel();
					deserializer.deserialize(myModel, delivery.getBody());
					
					AzureTablesTransformer att = new AzureTablesTransformer();
					AzureTablesModel fromMyModel = att.fromMyModel(myModel);
					List<DynamicTableEntity> entities = fromMyModel.getEntities();
					
					taskQueue.sendAck(delivery);
					
					String tableName = fromMyModel.getTableName();
					createTable(tableName);
					for(DynamicTableEntity entity : entities){
						insertEntity(tableName, entity);
						count++;
						if(count%2000==0)
							log.debug(Thread.currentThread().getName()+" Inserted "+count+" entities");
					}
					
				}else{
					log.debug(Thread.currentThread().getName() + " - The queue " +
							TaskQueue.getDefaultTaskQueueName() + " is empty");
				}
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException e) {
				log.error(Thread.currentThread().getName() + " - Cannot read next delivery from the queue " + 
					TaskQueue.getDefaultTaskQueueName(), e);
			} catch (TException e) {
				log.error(Thread.currentThread().getName() + " - Error deserializing message ", e);
			} catch (QueueException e) {
				log.error(Thread.currentThread().getName() + " - Error sending an acknowledgment to the queue " + 
						TaskQueue.getDefaultTaskQueueName(), e);
			} catch (URISyntaxException e) {
				log.error(Thread.currentThread().getName() + " - Error operating on Azure Tables ", e);
			} catch (StorageException e) {
				log.error(Thread.currentThread().getName() + " - Error storing data on Azure Tables ", e);
			}
		}
	}

	@Override
	protected Metamodel toMyModel(AbstractDatabase db) {
		Tables azure = (Tables) db;
		Iterable<String> tablesList = azure.getTablesList();
		
		for(String table : tablesList){
			TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
			ResultContinuation[] continuationToken = null;
			
			while(true){
				try {
					//ContinuationToken is passed by reference
					ArrayList<DynamicTableEntity> results = readSegment(table, continuationToken);
					for(DynamicTableEntity entity : results){
						AzureTablesModel model = new AzureTablesModel(table, entity);
						AzureTablesTransformer transformer = new AzureTablesTransformer();
						Metamodel myModel = transformer.toMyModel(model);
						taskQueue.publish(serializer.serialize(myModel));
					}
				} catch (TablesReadException e) {
					log.debug(e.getMessage());
					break;
				} catch (QueueException e) {
					log.error(Thread.currentThread().getName() + " - Error communicating with the queue " + 
							TaskQueue.getDefaultTaskQueueName(), e);
				} catch (TException e) {
					log.error(Thread.currentThread().getName() + " - Error serializing message ", e);
				}
				
			}
		}
		return null;
	}

	private ArrayList<DynamicTableEntity> readSegment(String tableName, ResultContinuation[] continuationToken) throws TablesReadException{
		int noEntities = 1000;
		ArrayList<DynamicTableEntity> results = new ArrayList<DynamicTableEntity>(noEntities);
		boolean proof = true;
		while(proof){
			try {
				ResultSegment<DynamicTableEntity> segment = 
						getEntities_withRange(tableName, continuationToken, noEntities);
				results = segment.getResults();
				continuationToken[0] = segment.getContinuationToken();
				proof = false;
				
				if(!segment.getHasMoreResults())
					throw new TablesReadException("No more entities to read");
			} catch (InvalidKeyException | URISyntaxException | IOException
					| StorageException e) {
				log.error(Thread.currentThread().getName() + " - Error reading segment from Azure Tables ", e);
			}
		}
		return results;
	}
	
	/**
	 * Checks if a connection has already been established
	 * @return true if connected, false if not.
	 */
	public boolean isConnected(){
		return (account==null || tableClient==null) ? false : true;
	}
	
	@Override
	public void connect() throws ConnectException {
		if(!isConnected()){
			String credentials = PropertiesManager.getCredentials(Constants.AZURE_PROP
					+".UNOFFICIAL");
			//log.debug(Constants.AZURE_PROP+" = "+credentials);
			try {
				account = CloudStorageAccount.parse(credentials);
				tableClient = account.createCloudTableClient();
				log.debug("Connected");
			} catch (InvalidKeyException | URISyntaxException e) {
				e.printStackTrace();
				throw new ConnectException(DefaultErrors.connectionError);
			}
		}else{
			throw new ConnectException(DefaultErrors.alreadyConnected);
		}
	}

	@Override
	public void disconnect() {
		if(isConnected()){
			account=null;
			tableClient=null;
			log.debug("Disconnected");
		}else{
			log.warn(DefaultErrors.notConnected);
		}
	}
	
	/**
	 * Creates the table in the storage service if it does not already exist.
	 *     
	 * @param tableName A String that represents the table name.
	 * @return The created table or null
	 * @throws URISyntaxException If the resource URI is invalid
	 */
	public CloudTable createTable(String tableName) throws URISyntaxException{
		
		if(tableName.indexOf("@")==0)
			tableName=tableName.substring(1);
		//log.debug("Creating table: "+tableName);
		if(isConnected()){
			CloudTable cloudTable = new CloudTable(tableName, tableClient);
			try {
				cloudTable.createIfNotExist();
			} catch (StorageException e) {
				e.printStackTrace();
			}
			return cloudTable;
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}

	/**
	 * Insert an entity in the given table, provided that the table exists
	 * @param tableName Name of the table where to insert the entity
	 * @param entity An instance of the DynamicEntity
	 * @throws StorageException - if an error occurs accessing the storage service, or the operation fails.
	 * 
	 * @return A TableResult containing the result of executing the TableOperation on the table. The TableResult class encapsulates the HTTP response and any table entity results returned by the Storage Service REST API operation called for a particular TableOperation.
	 */
	public TableResult insertEntity(String tableName, DynamicTableEntity entity) throws StorageException{
		if(tableName.indexOf("@")==0)
			tableName=tableName.substring(1);
		//log.debug("Inserting entity: "+entity.getRowKey()+" into "+tableName);
		if(isConnected()){
			/**
			 * Bullet proof write
			 */
			int retries = 0;
			
			while(true){
				try{
					// Create an operation to add the new entity.
					TableOperation insertion = TableOperation.insertOrMerge(entity);
			
					// Submit the operation to the table service.
					return tableClient.execute(tableName, insertion);
				}catch(Exception e){
					retries++;
					log.error(Thread.currentThread().getName() + 
							" ERROR -> insertEntity : "+e.getMessage());
					
					log.error("Entity "+entity.getRowKey(), e);
				}finally{
					if(retries>=5){
						log.error(Thread.currentThread().getName() + 
								"SKIPPING entity: "+entity.getRowKey());
						return null;
					}
				}
			}
			
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	/**
	 * Get a list of the tables contained by the Azure account
	 * @return A collection containing table's names as String
	 */
	public Iterable<String> getTablesList(){
		if(isConnected()){
			Iterable<String> listTables = tableClient.listTables();		
			return listTables;
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	/**
	 * Extracts the entities contained in a given table
	 * @param tableName The table name
	 * @return A collection containing table entities
	 */
	public Iterable<DynamicTableEntity> getEntitiesByTable(String tableName){
		if(isConnected()){
			TableQuery<DynamicTableEntity> partitionQuery =
				    TableQuery.from(tableName, DynamicTableEntity.class);
			return tableClient.execute(partitionQuery);
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	/**
	 * Extracts the entities contained in a given table in batches
	 * @param tableName The table name
	 * @param continuationToken The next point where to start fetching entities. Pass <code>null</code> at the beginning.
	 * @param pageSize The number of entities to retrieve. NB: 1000 max otherwise throws a <code> StorageException </code>
	 * @return A collection containing the entities, the continuationToken and other stuff.
	 * @throws InvalidKeyException
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws StorageException
	 */
	public ResultSegment<DynamicTableEntity> getEntities_withRange(String tableName,
			ResultContinuation[] continuationToken,
			int pageSize) throws InvalidKeyException, URISyntaxException, IOException, StorageException{
		if(isConnected()){
			TableQuery<DynamicTableEntity> partitionQuery =
				    TableQuery.from(tableName, DynamicTableEntity.class).take(pageSize);
			
			return tableClient.executeSegmented(partitionQuery, continuationToken[0]);
		}else{
			log.info(DefaultErrors.notConnected);
			return null;
		}
	}
	
	public CloudStorageAccount getAccount() {
		return account;
	}

	public void setAccount(CloudStorageAccount account) {
		this.account = account;
	}

	public CloudTableClient getTableClient() {
		return tableClient;
	}

	public void setTableClient(CloudTableClient tableClient) {
		this.tableClient = tableClient;
	}
}
