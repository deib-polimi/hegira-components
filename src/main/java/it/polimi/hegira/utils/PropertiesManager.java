package it.polimi.hegira.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesManager {
	private transient static Logger log = Logger.getLogger(PropertiesManager.class);
	
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
	
}
