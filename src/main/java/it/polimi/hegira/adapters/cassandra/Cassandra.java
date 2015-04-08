package it.polimi.hegira.adapters.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.models.Metamodel;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
/**
 * 
 * @author Andrea Celli
 *
 */
public class Cassandra extends AbstractDatabase {

	private List<ConnectionObject> connectionList;
	
	private class ConnectionObject{
		protected Session session;
		public ConnectionObject(){}
		public ConnectionObject(Session session){
			this.session=session;
		}
	}
	
	/**
	 * The constructor creates a ConnectionObject for each
	 * thread and adds it to the connectionList
	 * @param options
	 */
	public Cassandra(Map<String, String> options){
		super(options);
		if(THREADS_NO>0){
			connectionList = new ArrayList<ConnectionObject>(THREADS_NO);
			for(int i=0;i<THREADS_NO;i++)
				connectionList.add(new ConnectionObject());
		}else{
			connectionList = new ArrayList<ConnectionObject>(1);
			connectionList.add(new ConnectionObject());
		}
	}
	
	
	@Override
	protected AbstractDatabase fromMyModel(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected AbstractDatabase fromMyModelPartitioned(Metamodel mm) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Metamodel toMyModel(AbstractDatabase model) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void connect() throws ConnectException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Metamodel toMyModelPartitioned(AbstractDatabase model) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getTableList() {
		// TODO Auto-generated method stub
		return null;
	}

}
