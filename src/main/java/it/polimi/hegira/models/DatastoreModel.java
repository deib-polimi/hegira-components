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
/**
 * 
 */
package it.polimi.hegira.models;

import java.util.Map.Entry;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

/**
 * @author Marco Scavuzzo
 *
 */
public class DatastoreModel {
	//Entity objects contains Kind
	//At the end everything is inside the entity
	private Entity entity;
	//A key can be defined by a Name or by an Id
	//id default value is 0 (Long type)
	//Name default value is null
	private Key key;
	private long keyId;
	private String keyName;
	/*
	 * Entity groups are logical groupings of entities
	 * which permit transactions over multiple entities.
	 * Is defined by root entity and consist of root and all descendents
	 */
	private Iterable<Entity> ancestorPath;
	private String ancestorString;
	private Entry<?, ?> property;
	//test performance
	private Entity fictitiousEntity;
	
	public Entity getFictitiousEntity() {
		return fictitiousEntity;
	}

	public void setFictitiousEntity(Entity fictitiousEntity) {
		this.fictitiousEntity = fictitiousEntity;
	}

	public DatastoreModel(){
		
	}
	
	/**
	 * Constructs the model from a single entity
	 * @param entity An entity
	 */
	public DatastoreModel(Entity entity) {
		this.entity = entity;
	}

	/**
	 * Construct the model from an Entity Group (List of entities) in order to mantain transactionality
	 * @param ancestorPath A list of entities in the same ancestor path
	 */
	public DatastoreModel(Iterable<Entity> ancestorPath) {
		this.ancestorPath = ancestorPath;
	}

	public Entity getEntity() {
		return entity;
	}

	public void setEntity(Entity entity) {
		this.entity = entity;
	}

	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public long getKeyId() {
		return keyId;
	}

	public void setKeyId(long keyId) {
		this.keyId = keyId;
	}

	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	/**
	 * Gets an Iterable of entities in the same ancestor path
	 * @return
	 */
	public Iterable<Entity> getAncestorPath() {
		return ancestorPath;
	}

	public void setAncestorPath(Iterable<Entity> ancestorPath) {
		this.ancestorPath = ancestorPath;
	}

	public Entry<?, ?> getProperty() {
		return property;
	}

	public void setProperty(Entry<?, ?> property) {
		this.property = property;
	}

	public String getAncestorString() {
		return ancestorString;
	}

	public void setAncestorString(String ancestorString) {
		this.ancestorString = ancestorString;
	}
}
