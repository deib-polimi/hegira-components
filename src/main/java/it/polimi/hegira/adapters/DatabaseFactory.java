package it.polimi.hegira.adapters;

import it.polimi.hegira.adapters.datastore.Datastore;
import it.polimi.hegira.adapters.tables.Tables;
import it.polimi.hegira.utils.Constants;

import java.util.Map;

/**
 * Applying the Factory pattern to create proper database objects
 * @author Marco Scavuzzo
 *
 */
public class DatabaseFactory {
	/**
	* Get an instance of a database as a producer or as a consumer
	* @param name Name of the database to be instantiated
	* @param options A map containing the properties of the new db object to be created.
	* <code>mode</code> Consumer or Producer.
	* <code>threads</code> The number of consumer threads.
	* @return
	*/
	public static AbstractDatabase getDatabase(String name, Map<String, String> options){
		switch(name){
			case Constants.GAE_DATASTORE:
				return new Datastore(options);
			case Constants.AZURE_TABLES:
				return new Tables(options);
			default:
				return null;
		}
	}
}
