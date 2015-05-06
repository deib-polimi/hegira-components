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
		log.debug("trying to read credential properties");
		Properties properties=new Properties();
		URL resource = Thread.currentThread().getContextClassLoader().getResource(file);
		
		try{
			InputStream inputStream=new FileInputStream(resource.getFile());
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
