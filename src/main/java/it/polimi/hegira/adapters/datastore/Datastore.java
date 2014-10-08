package it.polimi.hegira.adapters.datastore;

import java.util.Map;

import it.polimi.hegira.adapters.AbstractDatabase;
import it.polimi.hegira.exceptions.ConnectException;
import it.polimi.hegira.models.Metamodel;

public class Datastore extends AbstractDatabase {

	public Datastore(Map<String, String> options) {
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

	@Override
	public void connect() throws ConnectException {
		// TODO Auto-generated method stub

	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub

	}

}
