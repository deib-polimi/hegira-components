package it.polimi.hegira.adapters.tables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;

import org.apache.log4j.Logger;

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

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;
import it.polimi.hegira.utils.PropertiesManager;

public class Tables extends AbstractDatabase {
	private transient Logger log = Logger.getLogger(Tables.class);
	private CloudStorageAccount account;
	private CloudTableClient tableClient;
	
	public Tables(Map<String, String> options) {
		super(options);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Metamodel toMyModel(AbstractDatabase model) {
		// TODO Auto-generated method stub
		return null;
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
			String credentials = PropertiesManager.getCredentials(Constants.AZURE_PROP);
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
			ResultContinuation continuationToken,
			int pageSize) throws InvalidKeyException, URISyntaxException, IOException, StorageException{
		if(isConnected()){
			TableQuery<DynamicTableEntity> partitionQuery =
				    TableQuery.from(tableName, DynamicTableEntity.class).take(pageSize);
			
			return tableClient.executeSegmented(partitionQuery, continuationToken);
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
