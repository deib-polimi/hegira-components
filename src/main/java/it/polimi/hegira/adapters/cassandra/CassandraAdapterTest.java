package it.polimi.hegira.adapters.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.adapters.DatabaseFactory;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.queue.TaskQueue;
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
	
	/**
	 * Create stubs for the TaskQueue
	 */
	@BeforeClass
	public static void init(){
		
		mockedTaskQueue=mock(TaskQueue.class);
		
		//Abstractact database, costruttore
		try{
        whenNew(TaskQueue.class).withAnyArguments().thenReturn(mockedTaskQueue);
		}catch(Exception e){
			System.out.println("error while stubbing the queue constructor");
		}
		
		//when(mockedTaskQueue.publish(Mockito.any(byte[].class))).thenReturn();

	}
	
	@Test
	public void connectionTest() {
		Map<String,String> optionsReader=new HashedMap();
		optionsReader.put("mode", "producer");
		optionsReader.put("threads", "1");
		optionsReader.put("queue-address", "none");
		
		Map<String,String> optionsWriter=new HashedMap();
		optionsWriter.put("mode", "consumer");
		optionsWriter.put("threads", "1");
		optionsWriter.put("queue-address", "none");
		
		reader=(Cassandra)DatabaseFactory.getDatabase("CASSANDRA", optionsReader);
		writer=(Cassandra)DatabaseFactory.getDatabase("CASSANDRA", optionsWriter);
		
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
		Session session=cluster.connect("test");
		
		String tableName="players";
		session.execute(
			      "CREATE TABLE IF NOT EXISTS " + tableName + " ( " + Constants.DEFAULT_PRIMARY_KEY_NAME + " varchar PRIMARY KEY,"
			      		+ "goal int,"
			      		+ "teams list<varchar> );");
			
		List<String> fakeList1=new ArrayList<String>();
		fakeList1.add("Real Madrid"); 
		fakeList1.add("Napoli");
		List<String> fakeList2=new ArrayList<String>();
		fakeList2.add("Manchester United"); 
		fakeList2.add("Real Madrid");
		
		Statement insert1 = QueryBuilder.insertInto("players").values(
	               new String[] { "id","goal","teams"},
	               new Object[] {"Callejon",9,fakeList1});
		   session.execute(insert1); 
		Statement insert2 = QueryBuilder.insertInto("players").values(
	               new String[] { "id","goal","teams"},
	               new Object[] {"Ronaldo",30,fakeList2});
		   session.execute(insert2);
		   
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
			Mockito.verify(mockedTaskQueue, Mockito.times(2)).publish(serializedRow.capture());
		} catch (QueueException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
		//check row 1
		//
		Metamodel result1=new Metamodel();
		try {
			deserializer.deserialize(result1, serializedRow.getAllValues().get(1));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Callejon",result1.getRowKey());
		assertEquals("@players#Callejon",result1.getPartitionGroup());
		
		assertEquals(result1.getColumns().get("players").size(),2);
		
		Column resultCol=result1.getColumns().get("players").get(0);
		Column listResult=result1.getColumns().get("players").get(1);
		assertEquals("goal",resultCol.getColumnName());
		assertEquals("teams",listResult.getColumnName());
		try {
			assertEquals(9,DefaultSerializer.deserialize(resultCol.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", resultCol.getColumnValueType());
		assertEquals(false,resultCol.isIndexable());
		//
		// check row 2
		//
		Metamodel result2=new Metamodel();
		try {
			deserializer.deserialize(result2, serializedRow.getAllValues().get(0));
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Ronaldo",result2.getRowKey());
		assertEquals("@players#Ronaldo",result2.getPartitionGroup());
		
		Column resultCol2=result2.getColumns().get("players").get(0);
		assertEquals("goal",resultCol2.getColumnName());
		try {
			assertEquals(30,DefaultSerializer.deserialize(resultCol2.getColumnValue()));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Integer", resultCol2.getColumnValueType());
		assertEquals(false,resultCol2.isIndexable());
		//
		// drops the table
		//
		//session.execute("DROP TABLE players");
		
	}

}
