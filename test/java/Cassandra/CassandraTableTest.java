package Cassandra;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import it.polimi.hegira.adapters.cassandra.SessionManager;
import it.polimi.hegira.adapters.cassandra.Table;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;
import it.polimi.hegira.transformers.ITransformer;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.PropertiesManager;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

import static org.mockito.Mockito.mock; 
import static org.mockito.Mockito.when;

/**
 * Test for the Table class in the Cassandra Adapter
 * In order to run the test there has to be an instance of Cassandra running on localhost.
 * A keyspace named 'test' has also to be created. 
 * 
 * Credential properties: server=127.0.0.1, keyspace=test
 *  
 * @author Andrea Celli
 *
 */
public class CassandraTableTest {
	
	private static Table table;
	
	@Test
	public void constructorTest() {
		String keyspace=PropertiesManager.getCredentials(Constants.CASSANDRA_KEYSPACE);
		
		
		try {
			table=new Table("users");
		} catch (ConnectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//check if a session has been created
		assertNotNull(table.getSession());
		
		//check id the table has been created
		TableMetadata myTable=table
				.getSession()
				.getCluster()
				.getMetadata()
				.getKeyspace(keyspace)
				.getTable("users");
		assertNotNull(myTable);
		
		//check if the table has the right name
		assertEquals(table.getTableName(),"users");
		
		//check if the table has the right amount of initial columns
		List<ColumnMetadata> initialColumns=myTable.getColumns();
		assertEquals(table.getColumns().size(),initialColumns.size());
	}
	
	
	
	@Test
	public void insertTest(){
		String keyspace=PropertiesManager.getCredentials(Constants.CASSANDRA_KEYSPACE);
		
		//create a new cassandra model instances
		CassandraModel row=new CassandraModel("users", "Bonaventura");
		row.addColumn(new CassandraColumn("gol", 3 , "int" , true));
		
		CassandraModel row2=new CassandraModel("users", "Eder");
		row2.addColumn(new CassandraColumn("gol", 9 , "int" , true));
		row2.addColumn(new CassandraColumn("notes", "good play", "varchar", false));
		
		CassandraModel row3=new CassandraModel("users", "Van Ginkel");
		row3.addColumn(new CassandraColumn("gol", 0 , "int" , true));
		
		CassandraModel row4=new CassandraModel("users", "Tevez");
		row4.addColumn(new CassandraColumn("gol", 13 , "int" , true));
		row4.addColumn(new CassandraColumn("notes", "average", "varchar", false));
		
		//perform the inserts
		try{
		table.insert(row);
		table.insert(row2);
		table.insert(row3);
		table.insert(row4);
		} catch(ClassNotFoundException ex){
			ex.printStackTrace();
		}
		
		TableMetadata myTable=table
				.getSession()
				.getCluster()
				.getMetadata()
				.getKeyspace(keyspace)
				.getTable("users");
		
		//
		// assertions
		//
		ResultSet results = table.getSession().execute("SELECT * FROM "+table.getTableName());
		//count the number of iteration (they should be equal to the number of rows)
		int iterations=0;
		for (Row resultRow : results) {
				iterations++;
			    Iterator<ColumnDefinitions.Definition> iterator=resultRow.getColumnDefinitions().iterator();
			    while(iterator.hasNext()){
				    ColumnDefinitions.Definition col=iterator.next();
				    String columnName=col.getName();
				    //for each row check the specific columns
				    String name=resultRow.getString(Constants.DEFAULT_PRIMARY_KEY_NAME);
				    switch(name){
				    	case "Bonaventura":
				    		if(columnName.equals("gol")){
				    			assertEquals(resultRow.getInt("gol"), 3);
				    			assertNotNull(myTable.getColumn(columnName).getIndex());
				    		}else{
				    			if(columnName.equals("notes")){
				    				assertNull(resultRow.getString(columnName));
				    			}
				    			else{
				    				if(!columnName.equals(Constants.DEFAULT_PRIMARY_KEY_NAME))
				    					fail("unexpected column");
				    			}
				    		}
				    		break;
				    	case "Eder":
				    		if(columnName.equals("gol")){
				    			assertEquals(resultRow.getInt("gol"), 9);
				    			assertNotNull(myTable.getColumn(columnName).getIndex());
				    		}else{
				    			if(columnName.equals("notes")){
				    				assertEquals(resultRow.getString("notes"), "good play");
				    				assertNull(myTable.getColumn(columnName).getIndex());
				    			}else{
				    			  if(!columnName.equals(Constants.DEFAULT_PRIMARY_KEY_NAME))
				    				  fail("unexpected column");
				    			}
				    		}
				    		break;
				    	case "Van Ginkel":
				    		if(columnName.equals("gol")){
				    			assertEquals(resultRow.getInt("gol"), 0);
				    			assertNotNull(myTable.getColumn(columnName).getIndex());
				    		}else{
				    			if(columnName.equals("notes")){
				    				assertNull(resultRow.getString(columnName));
				    			}
				    			else{
				    				if(!columnName.equals(Constants.DEFAULT_PRIMARY_KEY_NAME))
				    					fail("unexpected column");
				    			}
				    		}
				    		break;
				    	case "Tevez":
				    		if(columnName.equals("gol")){
				    			assertEquals(resultRow.getInt("gol"), 13);
				    			assertNotNull(myTable.getColumn(columnName).getIndex());
				    		}else{
				    			if(col.getName().equals("notes")){
				    				assertEquals(resultRow.getString("notes"), "average");
				    				assertNull(myTable.getColumn(columnName).getIndex());
				    			}else{
				    			   if(!columnName.equals(Constants.DEFAULT_PRIMARY_KEY_NAME))
				    				   fail("unexpected column");
				    			}
				    		}
				    		break;
				    		default: 
				    			fail("unexpected row");
		  }
	  	}
	  }
		//number of rows
		assertEquals(iterations, 4);
		
		
		//
		//tear down the connection and drop the table
		//
		Drop drop=SchemaBuilder.dropTable(table.getTableName());
		table.getSession().execute(drop);
		table.getSession().close();
	}
}	
