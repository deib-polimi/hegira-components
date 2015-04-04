package Cassandra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		Column col2=new Column("col2", ByteBuffer.wrap(DefaultSerializer.serialize(10)), "Integer", false);
		List<Column> list=new ArrayList<Column>();
		list.add(col);
		list.add(col2);
		Map<String,List<Column>> map=new HashMap<String,List<Column>>();
		map.put("firstFamily", list);
		meta.setColumns(map);
		}catch(IOException e){
			e.printStackTrace();
		}
		
		CassandraModel cm=ct.fromMyModel(meta);
		
		assertEquals(cm.getTable(),"firstFamily");
		assertEquals(cm.getKeyValue(),"11");
		assertEquals(cm.getColumns().size(),2);
		
		CassandraColumn cassCol1=cm.getColumns().get(0);
		assertEquals(cassCol1.getColumnName(),"col");
		assertEquals(cassCol1.getColumnValue(),"ABC");
		assertEquals(cassCol1.getValueType(),"varchar");
		assertEquals(cassCol1.isIndexed(),true);
		
		CassandraColumn cassCol2=cm.getColumns().get(1);
		assertEquals(cassCol2.getColumnName(),"col2");
		assertEquals(cassCol2.getColumnValue(),10);
		assertEquals(cassCol2.getValueType(),"int");
		assertEquals(cassCol2.isIndexed(),false);
	
	}
	
	/**
	 * Test the behaviour of fromMyModel with a not supported type
	 */
	@Test
	public void testNotSupportedTypeFromMyModel(){
		CassandraTransformer ct=new CassandraTransformer();
		
		Metamodel meta=new Metamodel();
		meta.setRowKey("11");
		meta.setPartitionGroup("partition");
		meta.addToColumnFamilies("firstFamily");
		try{
		Column col=new Column("col", ByteBuffer.wrap(DefaultSerializer.serialize("ABC")), "NotSupported", true);
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
		assertEquals(cm.getColumns().size(),2);
		CassandraColumn type=cm.getColumns().get(0);
		CassandraColumn serializedCol=cm.getColumns().get(1);
		assertEquals(type.getColumnName(),"col_Type");
		assertEquals(type.getColumnValue(),"NotSupported");
		assertEquals(type.getValueType(),"varchar");
		assertEquals(type.isIndexed(),false);
		assertEquals(serializedCol.getColumnName(),"col");
		try{
		assertEquals(serializedCol.getColumnValue(),ByteBuffer.wrap(DefaultSerializer.serialize("ABC")));
		}catch(IOException e){
			e.printStackTrace();
		}
		assertEquals(serializedCol.getValueType(),"blob");
		assertEquals(serializedCol.isIndexed(),true);
		
	}
	
	/**
	 * Test the behaviour of fromMyModel with a collection types
	 */
	@Test
	public void testCollectionsFromMyModel(){
        CassandraTransformer ct=new CassandraTransformer();
		
		Metamodel meta=new Metamodel();
		meta.setRowKey("11");
		meta.setPartitionGroup("partition");
		meta.addToColumnFamilies("firstFamily");
		
		Set set=new HashSet();
		set.add(2);
		List list=new ArrayList<Integer>();
		list.add(2);
		Map map=new HashMap<String,Integer>();
		map.put("due", 2);
		
		
		try{
		Column colSet=new Column("colSet", ByteBuffer.wrap(DefaultSerializer.serialize(set)), "Set<Integer>", true);
		Column colList=new Column("colList", ByteBuffer.wrap(DefaultSerializer.serialize(list)), "List<Integer>", true);
		Column colMap=new Column("colMap", ByteBuffer.wrap(DefaultSerializer.serialize(map)), "Map<String,Integer>", true);
		List<Column> cols=new ArrayList<Column>();
		cols.add(colSet);
		cols.add(colList);
		cols.add(colMap);
		Map<String,List<Column>> mapCols=new HashMap<String,List<Column>>();
		map.put("firstFamily", cols);
		meta.setColumns(map);
		}catch(IOException e){
			e.printStackTrace();
		}
		
		CassandraModel cm=ct.fromMyModel(meta);
		
		assertEquals(cm.getTable(),"firstFamily");
		assertEquals(cm.getKeyValue(),"11");
		assertEquals(cm.getColumns().size(),3);
		
		CassandraColumn cassCol1=cm.getColumns().get(0);
		assertEquals(cassCol1.getColumnName(),"colSet");
		assertEquals(cassCol1.getColumnValue(),set);
		assertEquals(cassCol1.getValueType(),"Set<int>");
		assertEquals(cassCol1.isIndexed(),true);
		
		CassandraColumn cassCol2=cm.getColumns().get(1);
		assertEquals(cassCol2.getColumnName(),"colList");
		assertEquals(cassCol2.getColumnValue(),list);
		assertEquals(cassCol2.getValueType(),"List<int>");
		assertEquals(cassCol2.isIndexed(),true);
		
		CassandraColumn cassCol3=cm.getColumns().get(2);
		assertEquals(cassCol3.getColumnName(),"colMap");
		assertEquals(cassCol3.getColumnValue(),map);
		assertEquals(cassCol3.getValueType(),"Map<varchar,int>");
		assertEquals(cassCol3.isIndexed(),true);
		
		
	}
	
}
