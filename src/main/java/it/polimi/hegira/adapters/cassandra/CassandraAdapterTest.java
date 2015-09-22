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
package it.polimi.hegira.adapters.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.adapters.DatabaseFactory;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
import it.polimi.hegira.utils.ConfigurationManagerCassandra;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultSerializer;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Any;
import org.mockito.internal.verification.Times;
import org.mockito.verification.VerificationMode;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.appengine.repackaged.org.apache.commons.collections.map.HashedMap;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import static org.powermock.api.mockito.PowerMockito.*;
import static org.mockito.Mockito.mock; 
import static org.mockito.Mockito.when;


/**
 * 
 * Test for the Cassandra Adapter
 * Tests will mock the behaviour of the TaskQueue
 * These tests require an instance of Cassandra running locally and a keyspace named test.
 * 
 * Credential properties: server=127.0.0.1, keyspace=test
 * 
 * @author Andrea Celli
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AbstractDatabase.class)
@PowerMockIgnore("javax.management.*")


public class CassandraAdapterTest {

	private  static TaskQueue mockedTaskQueue;
	private static Cassandra reader;
	private static Cassandra writer;
	private static QueueingConsumer mockedConsumer;
	private static Delivery mockedDelivery;
	private static byte[] toBeConsumed;
	private static Set<String> fakeSet;
	private static Session session; 
	private static String primaryKeyName;
	
	/**
	 * Create stubs for the TaskQueue
	 */
	@BeforeClass
	public static void init(){
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		
		primaryKeyName=ConfigurationManagerCassandra.getConfigurationProperties(Constants.PRIMARY_KEY_NAME);
		
		//
		//create the sample data to be inserted used in (from my model test)
		//
		Map<String,List<Column>> columns=new HashMap<String,List<Column>>();
		List<Column> columnsList=new ArrayList<Column>();
		Column col1=new Column();
		Column col2=new Column();
		col1.setColumnName("goal");
		col2.setColumnName("mates");
		fakeSet=new HashSet<String>();
		fakeSet.add("benzema"); fakeSet.add("marcelo");
		try {
			col1.setColumnValue(DefaultSerializer.serialize(30));
			col2.setColumnValue(DefaultSerializer.serialize(fakeSet));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		col1.setColumnValueType("Integer");
		col2.setColumnValueType("Set<String>");
		col1.setIndexable(false);
		col2.setIndexable(false);
		columnsList.add(col1);
		columnsList.add(col2);
		columns.put("players", columnsList);
		Metamodel toBeConsumedMeta=new Metamodel("@players#ronaldo", "ronaldo", columns);
		toBeConsumedMeta.getColumnFamilies().add("players");
		try {
			toBeConsumed=serializer.serialize(toBeConsumedMeta);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		mockedTaskQueue=mock(TaskQueue.class);
		mockedConsumer=mock(QueueingConsumer.class);
		mockedDelivery=mock(Delivery.class);
		
		//Abstractact database, costruttore
		//to mock the queue behavior in toMyModel
		try{
        whenNew(TaskQueue.class).withAnyArguments().thenReturn(mockedTaskQueue);
		}catch(Exception e){
			System.out.println("error while stubbing the queue constructor");
		}
		//to mock the queue behaviour in fromMyModel
		Mockito.when(mockedTaskQueue.getConsumer()).thenReturn(mockedConsumer);
		try {
			Mockito.when(mockedConsumer.nextDelivery()).thenReturn(mockedDelivery).thenReturn(null);
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Mockito.when(mockedDelivery.getBody()).thenReturn(toBeConsumed);

	}
	
	@Test
	public void connectionTest() {
		Map<String,String> optionsReader=new HashedMap();
		optionsReader.put("mode", "producer");
		optionsReader.put("threads", "1");
		optionsReader.put("queue-address", "none");
		
		Map<String,String> optionsWriter=new HashedMap();
		optionsWriter.put("mode", "consumer");
		optionsWriter.put("threads", "2");
		optionsWriter.put("queue-address", "none");
		
		reader=(Cassandra)DatabaseFactory.getDatabase("CASSANDRA", optionsReader);
		writer=(Cassandra)DatabaseFactory.getDatabase("CASSANDRA", optionsWriter);
		
		assertEquals(false, reader.isConnected());
		assertEquals(false, writer.isConnected());
		
		try{
			reader.connect();
			writer.connect();
		}catch(ConnectException e){
			e.printStackTrace();
			System.out.println("error while trying to connect");
		}
		
		assertEquals(true, reader.isConnected());
		assertEquals(true, writer.isConnected());
	}
	

	/**
	 * This test assumes the read consistency is set to eventual
	 */
	@Test
	public void toMyModelTest(){
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		
	    //
		// Connects to the db and creates two tables with some sample data for testing purposes
		// The table will be dropped at the end of the test
		//
		String hostname="127.0.0.1";
		Cluster.Builder clusterBuilder=Cluster.builder()
				.addContactPoint(hostname);
		Cluster cluster=clusterBuilder.build();
		session=cluster.connect("test");
		
		String firstTableName="players";
		String secondTableName="users";
		session.execute(
			      "CREATE TABLE IF NOT EXISTS " + firstTableName + " ( " + primaryKeyName + " varchar PRIMARY KEY,"
			      		+ "goal int,"
			      		+ "teams list<varchar> );");
		session.execute(
			      "CREATE TABLE IF NOT EXISTS " + secondTableName + " ( " + primaryKeyName + " varchar PRIMARY KEY,"
			      		+ "age int,"
			      		+ "contacts map<varchar,varchar> );");
			
		List<String> fakeList1=new ArrayList<String>();
		fakeList1.add("Real Madrid"); 
		fakeList1.add("Napoli");
		List<String> fakeList2=new ArrayList<String>();
		fakeList2.add("Manchester United"); 
		fakeList2.add("Real Madrid");
		
		Map<String,String> fakeMap1=new HashMap<String,String>();
		fakeMap1.put("Andrea","andrea@gmail.com");
		fakeMap1.put("Andre","andre@gmail.com");
		Map<String,String> fakeMap2=new HashMap<String,String>();
		fakeMap2.put("Luca","luca@gmail.com");
		fakeMap2.put("leo","leo@gmail.com");
		
		Statement insert1 = QueryBuilder.insertInto(firstTableName).values(
	               new String[] { "id","goal","teams"},
	               new Object[] {"Callejon",9,fakeList1});
		   session.execute(insert1); 
		Statement insert2 = QueryBuilder.insertInto(firstTableName).values(
	               new String[] { "id","goal","teams"},
	               new Object[] {"Ronaldo",30,fakeList2});
		   session.execute(insert2);
		   
		Statement insert3 = QueryBuilder.insertInto(secondTableName).values(
				new String[] {"id","age","contacts"},
				new Object[] {"Andrea",22,fakeMap1});
		session.execute(insert3);
		Statement insert4 = QueryBuilder.insertInto(secondTableName).values(
				new String[] {"id","age","contacts"},
				new Object[] {"Leo",1,fakeMap2});
		session.execute(insert4);		
		   
		//create the serialized column coresponding to the one read
		/*Column col=new Column();
		col.setColumnName("goal");
		try {
			col.setColumnValue(DefaultSerializer.serialize(9));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		col.setColumnValueType("Integer");
		col.setIndexable(false);
		
		List<Column> colList=new ArrayList<Column>();
		colList.add(col);
		
		Map<String,List<Column>> columns=new HashMap<String,List<Column>>();
		columns.put("test", colList);
	
		Metamodel metaColumn=new Metamodel("@players#Callejon", "Callejon", columns);*/
		
		ArgumentCaptor<byte[]> serializedRow=ArgumentCaptor.forClass(byte[].class);
		
		reader.toMyModel(null);
		
		/*try {
			Mockito.verify(mockedTaskQueue).publish(serializer.serialize(metaColumn));
		} catch (QueueException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
		try {
			Mockito.verify(mockedTaskQueue, Mockito.times(4)).publish(serializedRow.capture());
		} catch (QueueException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
		//check user 1
		//
		Metamodel resultUser1=new Metamodel();
		try {
			deserializer.deserialize(resultUser1, serializedRow.getAllValues().get(0));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Leo",resultUser1.getRowKey());
		assertEquals("@users#Leo",resultUser1.getPartitionGroup());
		assertEquals(resultUser1.getColumns().get("users").size(),2);
		assertEquals(1, resultUser1.getColumnFamilies().size());
		assertEquals("users",resultUser1.getColumnFamilies().get(0));
		Column userAge=resultUser1.getColumns().get("users").get(0);
		Column contactsUser=resultUser1.getColumns().get("users").get(1);
		assertEquals("age",userAge.getColumnName());
		assertEquals("contacts",contactsUser.getColumnName());
		try {
			assertEquals(1,DefaultSerializer.deserialize(userAge.getColumnValue()));
			assertEquals(fakeMap2, DefaultSerializer.deserialize(contactsUser.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", userAge.getColumnValueType());
		assertEquals("Map<String,String>", contactsUser.getColumnValueType());
		assertEquals(false,userAge.isIndexable());
		assertEquals(false,contactsUser.isIndexable());
		//
		//check user 2
		//
		Metamodel resultUser2=new Metamodel();
		try {
			deserializer.deserialize(resultUser2, serializedRow.getAllValues().get(1));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Andrea",resultUser2.getRowKey());
		assertEquals("@users#Andrea",resultUser2.getPartitionGroup());
		assertEquals(resultUser2.getColumns().get("users").size(),2);
		Column userAge2=resultUser2.getColumns().get("users").get(0);
		Column contactsUser2=resultUser2.getColumns().get("users").get(1);
		assertEquals("age",userAge2.getColumnName());
		assertEquals("contacts",contactsUser2.getColumnName());
		try {
			assertEquals(22,DefaultSerializer.deserialize(userAge2.getColumnValue()));
			assertEquals(fakeMap1, DefaultSerializer.deserialize(contactsUser2.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", userAge2.getColumnValueType());
		assertEquals("Map<String,String>", contactsUser2.getColumnValueType());
		assertEquals(false,userAge2.isIndexable());
		assertEquals(false,contactsUser2.isIndexable());
		//
		//check players row 1
		//
		Metamodel resultPlayer1=new Metamodel();
		try {
			deserializer.deserialize(resultPlayer1, serializedRow.getAllValues().get(3));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Callejon",resultPlayer1.getRowKey());
		assertEquals("@players#Callejon",resultPlayer1.getPartitionGroup());
		
		assertEquals(resultPlayer1.getColumns().get("players").size(),2);
		
		Column resultCol=resultPlayer1.getColumns().get("players").get(0);
		Column listResult=resultPlayer1.getColumns().get("players").get(1);
		assertEquals("goal",resultCol.getColumnName());
		assertEquals("teams",listResult.getColumnName());
		try {
			assertEquals(9,DefaultSerializer.deserialize(resultCol.getColumnValue()));
			assertEquals(fakeList1, DefaultSerializer.deserialize(listResult.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", resultCol.getColumnValueType());
		assertEquals("List<String>", listResult.getColumnValueType());
		assertEquals(false,resultCol.isIndexable());
		assertEquals(false,listResult.isIndexable());
		//
		// check players row 2
		//
		Metamodel resultPlayer2=new Metamodel();
		try {
			deserializer.deserialize(resultPlayer2, serializedRow.getAllValues().get(2));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Ronaldo",resultPlayer2.getRowKey());
		assertEquals("@players#Ronaldo",resultPlayer2.getPartitionGroup());
		
		assertEquals(resultPlayer2.getColumns().get("players").size(),2);
		
		Column resultCol2=resultPlayer2.getColumns().get("players").get(0);
		Column listResult2=resultPlayer2.getColumns().get("players").get(1);
		assertEquals("goal",resultCol2.getColumnName());
		assertEquals("teams",listResult2.getColumnName());
		try {
			assertEquals(30,DefaultSerializer.deserialize(resultCol2.getColumnValue()));
			assertEquals(fakeList2, DefaultSerializer.deserialize(listResult2.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", resultCol2.getColumnValueType());
		assertEquals("List<String>", listResult2.getColumnValueType());
		assertEquals(false,resultCol2.isIndexable());
		assertEquals(false,listResult2.isIndexable());

		//
		// drops the table
		//
		session.execute("DROP TABLE "+firstTableName);
		session.execute("DROP TABLE "+secondTableName);
	}
	
	@Test
	public void fromMyModel(){
		
		
		
		writer.fromMyModel(null);
		
		
		ArgumentCaptor<byte[]> serializedRow=ArgumentCaptor.forClass(byte[].class);
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		
		reader.toMyModel(null);
		
		try {
			Mockito.verify(mockedTaskQueue, Mockito.times(5)).publish(serializedRow.capture());
		} catch (QueueException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Metamodel result=new Metamodel();
		try {
			deserializer.deserialize(result, serializedRow.getAllValues().get(4));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("ronaldo",result.getRowKey());
		assertEquals("@players#ronaldo",result.getPartitionGroup());
		
		assertEquals(result.getColumns().get("players").size(),2);
		
		Column resultCol=result.getColumns().get("players").get(0);
		Column set=result.getColumns().get("players").get(1);
		assertEquals("goal",resultCol.getColumnName());
		assertEquals("mates",set.getColumnName());
		try {
			assertEquals(30,DefaultSerializer.deserialize(resultCol.getColumnValue()));
			assertEquals(fakeSet, DefaultSerializer.deserialize(set.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", resultCol.getColumnValueType());
		assertEquals("Set<String>", set.getColumnValueType());
		assertEquals(false,resultCol.isIndexable());
		assertEquals(false,set.isIndexable());
		
		session.execute("DROP TABLE players");

	}

}
