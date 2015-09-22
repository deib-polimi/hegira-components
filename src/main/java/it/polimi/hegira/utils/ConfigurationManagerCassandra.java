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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ConfigurationManagerCassandra {
	private static Logger log=Logger.getLogger(ConfigurationManagerCassandra.class);
	
	private static String getPropertiesFromFile(String file,String propertyKey){
		log.debug("trying to read configuration info!!!");
		Properties properties=new Properties();
		//URL resource = Thread.currentThread().getContextClassLoader().getResource(file);
		InputStream inputStream = PropertiesManager.class.getResourceAsStream("/"+file);
		try{
			//InputStream inputStream=new FileInputStream(resource.getFile());
			properties.load(inputStream);
			return properties.getProperty(propertyKey);
		}catch(FileNotFoundException | NullPointerException e){
			log.error(file+" file has to exist");
		}catch(IOException e){
			log.error("Unable to read file "+file);
		}finally{
			properties=null;
		}
		
		return null;
	}
	
	public static String getConfigurationProperties(String propertyKey){
		return getPropertiesFromFile(Constants.CASSANDRA_CONFIGURATION_FILE, propertyKey);
	}
}
