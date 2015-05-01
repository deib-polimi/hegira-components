package it.polimi.hegira.adapters.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.appengine.repackaged.org.apache.commons.collections.map.HashedMap;

import static org.powermock.api.mockito.PowerMockito.*;
import static org.mockito.Mockito.mock; 
import static org.mockito.Mockito.when;


/**
 * 
 * Test for the Cassandra Adapter
 * Tests will mock the behaviour of the TaskQueue
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
		
		//create the serialized column coresponding to the one read
		
		Column col=new Column();
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
	
		Metamodel metaColumn=new Metamodel("@players#Callejon", "Callejon", columns);
		
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
			Mockito.verify(mockedTaskQueue).publish(serializedRow.capture());
		} catch (QueueException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Metamodel result=new Metamodel();
		try {
			deserializer.deserialize(result, serializedRow.getValue());
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertEquals("Callejon",result.getRowKey());
		assertEquals("@players#Callejon",result.getPartitionGroup());
		
		Column resultCol=result.getColumns().get("players").get(0);
		assertEquals("goal",resultCol.getColumnName());
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

	}

}
