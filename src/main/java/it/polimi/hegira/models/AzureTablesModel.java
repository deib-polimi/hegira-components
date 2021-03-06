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

import java.util.List;

import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;

/**
 * @author Marco Scavuzzo
 *
 */
public class AzureTablesModel {
	private String tableName;
	private DynamicTableEntity entity;
	//Entities contained by the same table
	private List<DynamicTableEntity> entities;
	
	public AzureTablesModel(){
		
	}
	
	public AzureTablesModel(String tableName, DynamicTableEntity entity){
		this.tableName = tableName;
		this.entity = entity;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public DynamicTableEntity getEntity() {
		return entity;
	}

	public void setEntity(DynamicTableEntity entity) {
		this.entity = entity;
	}

	public List<DynamicTableEntity> getEntities() {
		return entities;
	}

	public void setEntities(List<DynamicTableEntity> entities) {
		this.entities = entities;
	}
}
