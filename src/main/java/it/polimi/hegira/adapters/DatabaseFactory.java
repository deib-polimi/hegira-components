/**
 * Copyright 2015 Marco Scavuzzo
 * Contact: Marco Scavuzzo <marco.scavuzzo@polimi.it>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.polimi.hegira.adapters;

import it.polimi.hegira.adapters.cassandra.Cassandra;
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
			case Constants.CASSANDRA:
				return new Cassandra(options);
			default:
				return null;
		}
	}
}
