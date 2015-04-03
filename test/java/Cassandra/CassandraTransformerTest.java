package Cassandra;

import static org.junit.Assert.*;
import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.models.Metamodel;
import it.polimi.hegira.transformers.CassandraTransformer;

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
		row.addColumn(new CassandraColumn("column2", "stringa", "String", false));
		
		CassandraTransformer transf=new CassandraTransformer("strong");
		Metamodel meta=transf.toMyModel(row);
		
		assertEquals("007",meta.getRowKey());
		assertEquals(1,meta.getColumnFamilies().size());
		assertEquals("defaultTable",meta.getColumnFamilies().get(0));
		assertEquals("@strong#strong",meta.getPartitionGroup());
		assertEquals(2,meta.getColumns().get("defaultTable").size());
	
	}
	
}
