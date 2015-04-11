package it.polimi.hegira.utils;

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
			if(collectionType.equals("Map")||collectionType.equals("List") || collectionType.equals("Set")){
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
}
