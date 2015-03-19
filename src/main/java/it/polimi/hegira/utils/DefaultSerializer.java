/**
 * 
 */
package it.polimi.hegira.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Class providing the standard Java (de)serialization.
 * To be used when converting from and to the intermediate meta-model properties.
 * @author Marco Scavuzzo
 */
public class DefaultSerializer {
	/**
	 * Serializes the given object.
	 * @param obj The object to be serialized.
	 * @return An array of bytes representing the given object.
	 * @throws IOException
	 */
	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);
		return b.toByteArray();
	}
	
	/**
	 * Deserializes an array of bytes given as input.
	 * @param bytes The array of bytes.
	 * @return The deserialized object.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
		ObjectInputStream o = new ObjectInputStream(b);
		return o.readObject();
	}
}
