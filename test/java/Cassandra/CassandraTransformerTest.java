package Cassandra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Column;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.transformers.CassandraTransformer;
import it.polimi.hegira.utils.DefaultSerializer;

import org.junit.Test;

/**
 * 
 * @author Andrea Celli
 *
 */
public class CassandraTransformerTest {

	/**
	 * Test method for ToMyModel with strong consistency
	 */
	@Test 
	public void testToMyModelStrong(){
		//create a new Cassandra row to be "translated"
		CassandraModel row=new CassandraModel("defaultTable", "007");
		row.addColumn(new CassandraColumn("column1", 1001, "Integer", true));
		
		CassandraTransformer transf=new CassandraTransformer("strong");
		Metamodel meta=transf.toMyModel(row);
		
		assertEquals("007",meta.getRowKey());
		assertEquals(1,meta.getColumnFamilies().size());
		assertEquals("defaultTable",meta.getColumnFamilies().get(0));
		assertEquals("@strong#strong",meta.getPartitionGroup());
		assertEquals(1,meta.getColumns().get("defaultTable").size());
		Column translated=meta.getColumns().get("defaultTable").get(0);
		assertEquals("column1",translated.getColumnName());
		assertEquals("Integer",translated.getColumnValueType());
		try{
		//assertEquals(DefaultSerializer.serialize(1001),translated.getColumnValue());
		assertEquals(1001,DefaultSerializer.deserialize(translated.getColumnValue()));
		}catch (IOException | ClassNotFoundException e){
			e.printStackTrace();
		}
		assertEquals(true,translated.isIndexable());
	}
	
	/**
	 * Test toMyModel with a row containing an empty column
	 */
	@Test
	public void testEmptyColumnToMyModel(){
		CassandraModel row=new CassandraModel("defaultTable", "007");
		row.addColumn(new CassandraColumn("column1", null, "Integer", true));
		

		CassandraTransformer transf=new CassandraTransformer("strong");
		Metamodel meta=transf.toMyModel(row);
		
		assertEquals("007",meta.getRowKey());
		assertEquals(1,meta.getColumnFamilies().size());
		assertEquals("defaultTable",meta.getColumnFamilies().get(0));
		assertEquals("@strong#strong",meta.getPartitionGroup());
		assertEquals(0,meta.getColumns().get("defaultTable").size());
	}
	
	/**
	 * Test for eventual consistency
	 */
	@Test 
	public void testEventualConsistencyToMyModel(){
		CassandraTransformer ct=new CassandraTransformer("eventual");
		CassandraModel row0=new CassandraModel("defaultTable","007");
		CassandraModel row1=new CassandraModel("otherTable", "1");
		
		Metamodel meta0=ct.toMyModel(row0);
		Metamodel meta1=ct.toMyModel(row1);
		
		assertEquals("@defaultTable#007",meta0.getPartitionGroup());
		assertEquals("@otherTable#1",meta1.getPartitionGroup());
		
	}
	
	/**
	 * Test fromMyModel
	 */
	@Test
	public void testFromMyModel(){
		CassandraTransformer ct=new CassandraTransformer();
		
		Metamodel meta=new Metamodel();
		meta.setRowKey("11");
		meta.setPartitionGroup("partition");
		meta.addToColumnFamilies("firstFamily");
		meta.addToColumnFamilies("second");
		try{
		Column col=new Column("col", ByteBuffer.wrap(DefaultSerializer.serialize("ABC")), "String", true);
		List<Column> list=new ArrayList<Column>();
		list.add(col);
		Map<String,List<Column>> map=new HashMap<String,List<Column>>();
		map.put("firstFamily", list);
		meta.setColumns(map);
		}catch(IOException e){
			e.printStackTrace();
		}
		
		CassandraModel cm=ct.fromMyModel(meta);
		
		assertEquals(cm.getTable(),"firstFamily");
		assertEquals(cm.getKeyValue(),"11");
		assertEquals(cm.getColumns().size(),1);
		CassandraColumn cassCol=cm.getColumns().get(0);
		assertEquals(cassCol.getColumnName(),"col");
		assertEquals(cassCol.getColumnValue(),"ABC");
		assertEquals(cassCol.getValueType(),"varchar");
		assertEquals(cassCol.isIndexed(),true);
	
	}
	
}
