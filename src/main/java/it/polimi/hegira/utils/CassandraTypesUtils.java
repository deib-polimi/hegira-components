package it.polimi.hegira.utils;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;

import it.polimi.hegira.models.CassandraColumn;

/**
 * This class contains methods to manage type conversions between java types and CQL types.
 * 
 * @author Andrea Celli
 *
 */
public class CassandraTypesUtils {
	
	/**
	 * @param type - a collection type in the form X<Y,Z>
	 * @return the collection type name
	 */
	public static String getCollectionType(String type){
		return type.substring(0, type.indexOf("<"));
	}
	
	/**
	 * Returns the first type contained as a sub type of a MAP
	 *@param completeType - the complete collection type in the form X<Y,Z>
	 *@return String the first subtype of the collection
	 * @throws ClassNotFoundException
	 */
	public static String getFirstSimpleType(String completeType) throws ClassNotFoundException{
		if(completeType.contains("<") && completeType.contains(">")){
			return completeType.substring(completeType.indexOf("<")+1,completeType.indexOf(","));
		}else
		throw new ClassNotFoundException();
	}
	
	/**
	 * Returns the second type contained as a sub type of a MAP
	 *@param completeType - the complete collection type in the form X<Y,Z>
	 *@return the second subtype of the collection
	 * @throws ClassNotFoundException
	 */
	public static String getSecondSimpleType(String completeType) throws ClassNotFoundException{
		if(completeType.contains("<") && completeType.contains(">") ){
			return completeType.substring(completeType.indexOf(",")+1,completeType.indexOf(">"));
		}else
		throw new ClassNotFoundException();
	}
	
	/**
	 * @param type - the complete collection type in the form X<Y,Z>
	 * @return true if the type is one of the collections supported by Cassandra (map,list,set)
	 * @throws ClassNotFoundException
	 */
	public static boolean isSupportedCollection(String type) throws ClassNotFoundException{
		//I suppose the supported types are in the form Map<T,K>,List<T>,Set<T>
		if(isCollection(type)){
			String collectionType=CassandraTypesUtils.getCollectionType(type);
			if(collectionType.equals("Map") || 
					collectionType.equals("map") ||
					collectionType.equals("List") || 
					collectionType.equals("list") ||
					collectionType.equals("Set") ||
					collectionType.equals("set")){
				return true;
			}else
				//if the string contains "<,>" but is not of one of the supported types then it can not be recognized
				throw new ClassNotFoundException();
		}
		return false;
	}
	
	/**
	 * @param type
	 * @return true - if the type is a collection
	 */
	public static boolean isCollection(String type){
		return type.contains("<") && type.contains(">");
	}
	
	/**
	 * Check if the simple type (java type) is supported by cassandra (simple means "not a collection").
	 * If the type is supported the method does not perform any action.
	 * If the type is NOT supported the method throws an exception in order to handle the problem at
     * a higher level
	 *@param type
	 *@throws ClassNotFoundException 
	 */
	public static void checkIfSupported(String simpleJavaType) throws ClassNotFoundException{
		if(!(
				simpleJavaType.equals("String")||	
				simpleJavaType.equals("Long")||	
				simpleJavaType.equals("byte[]")||	
				simpleJavaType.equals("Boolean")||	
				simpleJavaType.equals("BigDecimal")||	
				simpleJavaType.equals("Double")||	
				simpleJavaType.equals("Float")||		
				simpleJavaType.equals("InetAddress")||
				simpleJavaType.equals("Integer")||
				simpleJavaType.equals("Date")||
				simpleJavaType.equals("UUID") ||
				simpleJavaType.equals("BigInteger"))){
			throw new ClassNotFoundException();
		}
	}
	
	/**
	 * Translate the java type of the collection in a String representing the CQL type
	 * @param cassandraColumn
	 * @param javaType
	 * @throws ClassNotFoundException
	 */
	public static void translateCollectionType (CassandraColumn cassandraColumn,
			String javaType) throws ClassNotFoundException {
		String collectionType=CassandraTypesUtils.getCollectionType(javaType);
		String cqlType=new String();
		if(collectionType.equals("Map")){
			String firstSimpleType=translateSimpleType(CassandraTypesUtils.getFirstSimpleType(javaType));
			String secondSimpleType=translateSimpleType(CassandraTypesUtils.getSecondSimpleType(javaType));
			cqlType=collectionType+"<"+firstSimpleType+","+secondSimpleType+">";
		}else{
			String simpleType=translateSimpleType(javaType.substring(javaType.indexOf("<")+1, javaType.indexOf(">")));
			cqlType=collectionType+"<"+simpleType+">";
		}
		cassandraColumn.setValueType(cqlType);
	}

	/**
	 * from java simple type to CQL simple type
	 * @param type
	 * @return CQL simple type
	 * @throws ClassNotFoundException
	 */
	public static String translateSimpleType(String type) throws ClassNotFoundException{
		switch(type){
		case "String":
			return "varchar";
		case "Long":
			return "bigint";
		case "byte[]":
			return "blob";
		case "Boolean":
			return "boolean";
		case "BigDecimal":
			return "decimal";
		case "Double":
			return "double";
		case "Float":
			return "float";
		case "InetAddress":
			return "inet";
		case "Integer":
			return "int";
		case "Date":
			return "timestamp";
		case "UUID":
			return "uuid";
		case "BigInteger":
			return "varint";
		default: 
			throw  new ClassNotFoundException();
	}
	}
	
	/**
	 * CQL simple data type--->java type
	 * @param CQLSimpleType
	 * @return string containing the simple java type 
	 * @throws ClassNotFoundException
	 */
	public static String getJavaSimpleType(String CQLSimpleType) throws ClassNotFoundException{
		switch(CQLSimpleType){
		case "ascii": 
			return "String";
		case "bigint":
			return "Long";
		case "blob":
			//TODO: check
			return "byte[]";
		case "boolean":
			return "Boolean";
		case "counter":
			return "Long";
		case "decimal":
			return "BigDecimal";
		case "double":
			return "Double";
		case "float":
			return "Float";
		case "inet":
			return "InetAddress";
		case "int":
			return "Integer";
		case "text":
			return "String";
		case "timestamp":
			return "Date";
		case "uuid":
			return "UUID";
		case "varchar":
			return "String";
		case "varint":
			return "BigInteger";
		case "timeuuid":
			return "UUID";
		case "udt":
			return "UDTValue";
		case "tuple":
			return "TupleValue";
		case "custom":
			//TODO: check
			return "ByteBuffer";
		default: 
			throw  new ClassNotFoundException();
		}
	}
	
	/**
	 * Retrieves a simple CQL datatype class from its name given as a string
	 * @param type
	 * @return the DataType class
	 * @throws ClassNotFoundException
	 */
	private static DataType getCQLSimpleDataType(String type) throws ClassNotFoundException{
		switch(type){
		case "ascii": 
			return DataType.ascii();
		case "bigint":
			return DataType.bigint();
		case "blob":
			return DataType.blob();
		case "boolean":
			return DataType.cboolean();
		case "counter":
			return DataType.counter();
		case "decimal":
			return DataType.decimal();
		case "double":
			return DataType.cdouble();
		case "float":
			return DataType.cfloat();
		case "inet":
			return DataType.inet();
		case "int":
			return DataType.cint();
		case "text":
			return DataType.text();
		case "timestamp":
			return DataType.timestamp();
		case "uuid":
			return DataType.uuid();
		case "varchar":
			return DataType.varchar();
		case "varint":
			return DataType.varint();
		case "timeuuid":
			return DataType.timeuuid();
		case "udt":
			//TODO
			throw  new ClassNotFoundException();
		case "tuple":
			//TODO
			throw  new ClassNotFoundException();
		case "custom":
			//TODO: check
			return DataType.blob();
		default: 
			throw  new ClassNotFoundException();
		
		}
	}
	
	/**
	 * Returns the CQL DataType class corresponding to a collection
	 * @param type - the string specifying the type in the form Map<K,X> or Set<K> or List<K>
	 * @param subTypes - DataTypes of the subtypes
	 * @return dataType
	 * @throws ClassNotFoundException
	 * @throws InvalidParameterException
	 */
	private static DataType getCollectionCQLDataType(String type,List<DataType> subTypes) throws ClassNotFoundException,InvalidParameterException{
		String collectionType=getCollectionType(type);
		if(collectionType.equals("Map")){
			if(subTypes.size()==2){
				return DataType.map(subTypes.get(0), subTypes.get(1));
			}else{
				throw new InvalidParameterException();
			}
		}else{
			if(subTypes.size()==1){
				if(collectionType.equals("List")){
					return DataType.list(subTypes.get(0));
				}
				if(collectionType.equals("Set")){
					return DataType.set(subTypes.get(0));
				}
			}else{
				throw new InvalidParameterException();
			}
		}
		throw new ClassNotFoundException();
	}
	
	/**
	 * Returns the CQL DataType specified by a given string
	 * Puts togheter the simple type case and the collection case.
	 * @param type
	 * @return the DataType
	 * @throws ClassNotFoundException
	 * @throws InvalidParameterException
	 */
	public static DataType getCQLDataType(String type) throws ClassNotFoundException,InvalidParameterException{
		if(isCollection(type)){
			String collectionType=getCollectionType(type);
			List<DataType> subTypes=new ArrayList<DataType>();
			
			if(collectionType.equals("Map")){
				String sub1,sub2;
				 sub1=getFirstSimpleType(type);
				 sub2=getSecondSimpleType(type);
				 
				 subTypes.add(getCQLSimpleDataType(sub1));
				 subTypes.add(getCQLSimpleDataType(sub2));
			}else{
				String sub=type.substring(type.indexOf("<")+1, type.indexOf(">"));
				subTypes.add(getCQLSimpleDataType(sub));
			}
			
			return getCollectionCQLDataType(type, subTypes);
		}else
			//simple type
			return getCQLSimpleDataType(type);
	}
}
