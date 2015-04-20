package Cassandra;

import static org.junit.Assert.*;
import it.polimi.hegira.adapters.cassandra.SessionManager;
import it.polimi.hegira.adapters.cassandra.Table;
import it.polimi.hegira.models.CassandraColumn;
import it.polimi.hegira.models.CassandraModel;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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

	/*private static SessionManager sessionManager;
	private static Session session;
	private static Cluster cluster;
	
	@BeforeClass
	public static void setUp(){
		//creating the stub for the SessionManager object
		sessionManager=mock(SessionManager.class);	
		
		//create the session to be used for test purposes 
		String hostname="127.0.0.1";
		Cluster.Builder clusterBuilder=Cluster.builder()
				.addContactPoint(hostname);
		cluster=clusterBuilder.build();
		//we suppose the local instance of cassandra to have a keyspace named 'test'
		session=cluster.connect("test");
		
		
		when(SessionManager.getSessionManager()).thenAnswer(new Answer<Session>() {
		     public Session answer(InvocationOnMock invocation) throws Throwable {
		         return session;
		     }
		 });
	}*/
	
	private static Table table;
	
	@Test
	public void constructorTest() {
		
		table=new Table("users");
		
	}
	
	@Test
	public void insertTest(){
		
		//create a new cassandra model instance
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
		
		//perform the insert
		try{
		table.insert(row);
		table.insert(row2);
		table.insert(row3);
		table.insert(row4);
		} catch(ClassNotFoundException ex){}
	}

}
