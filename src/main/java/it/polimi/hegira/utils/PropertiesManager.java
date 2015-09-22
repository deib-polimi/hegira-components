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
package it.polimi.hegira.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
/**
 * Utility class to retrieve data from properties files.
 * @author Marco Scavuzzo
 *
 */
public class PropertiesManager {
	private transient static Logger log = Logger.getLogger(PropertiesManager.class);
	
	/**
	 * Gets the value of a given property stored inside the credentials file.
	 * @param property	The name of the property.
	 * @return	The value for the given property name.
	 */
	public static String getCredentials(String property){
		Properties props = new Properties();
		try {
			log.debug("Trying to read "+Constants.CREDENTIALS_PATH);
			//URL systemResource = Thread.currentThread().getContextClassLoader().getResource(Constants.CREDENTIALS_PATH);
			//log.debug("Loaded "+systemResource);
			//InputStream isr = new FileInputStream(systemResource.getFile());
			InputStream isr = PropertiesManager.class.getResourceAsStream("/"+Constants.CREDENTIALS_PATH);
			
			if (isr == null){
				throw new FileNotFoundException(Constants.CREDENTIALS_PATH+" must exist.");
			}
			props.load(isr);
			return props.getProperty(property);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			return null;
		} finally {
			props=null;
		}
		return null;
	}
	
	/**
	 * Gets the ZooKeeper connect string from the credentials file.
	 * @return	ZooKeeper connect string (i.e., ip_address:port).
	 */
	public static String getZooKeeperConnectString(){
		return getCredentials(Constants.ZK_CONNECTSTRING);
	}
	
}
