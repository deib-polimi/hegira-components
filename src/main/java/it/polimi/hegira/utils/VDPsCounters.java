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
package it.polimi.hegira.utils;

import java.util.HashMap;

/**
 * Class representing the counters a TWC has to increment upon receiving a meta-model entity, 
 * when migrating in a partitioned eay.
 * @author Marco Scavuzzo
 *
 */
public class VDPsCounters {

	/**
	 * K - column family name
	 * V - MapVDPcounters: counters for all VDPids relative to that cf
	 */
	private volatile HashMap<String, MapVDPcounters> cfCounters;
	
	/**
	 * Instantiates the counters if they haven't already been.
	 */
	public VDPsCounters() {
		if(cfCounters==null)
			cfCounters = new HashMap<String, MapVDPcounters>();
	}
	
	/**
	 * Atomically increments the counter for a given VDP.
	 * If necessary, it also creates the map containing the counters.
	 * @param cf The column family that contains the VDP to increment.
	 * @param VDPid The id of the VDP whose counter should be incremented.
	 * @return The updated value of the counter.
	 */
	public int putAndIncrementCounter(String cf, Integer VDPid){
		synchronized(cfCounters){
			MapVDPcounters mapVDPcounters = cfCounters.get(cf);
			mapVDPcounters = (mapVDPcounters==null) ? new MapVDPcounters() : mapVDPcounters;
			int updatedValue = mapVDPcounters.putAndIncrement(VDPid);
			cfCounters.put(cf, mapVDPcounters);
			return updatedValue;
		}
	}
	
	/**
	 * Atomically increments the counter for a given VDP.
	 * If necessary, it also creates the map of the given size containing the counters.
	 * @param cf The column family that contains the VDP to increment.
	 * @param VDPid The id of the VDP whose counter should be incremented.
	 * @param size The number of VDPs each cf should contain.
	 * @return The updated value of the counter.
	 */
	public int putAndIncrementCounter(String cf, Integer VDPid, Integer size){
		synchronized(cfCounters){
			MapVDPcounters mapVDPcounters = cfCounters.get(cf);
			mapVDPcounters = (mapVDPcounters==null) ? new MapVDPcounters(size) : mapVDPcounters;
			int updatedValue = mapVDPcounters.putAndIncrement(VDPid);
			cfCounters.put(cf, mapVDPcounters);
			return updatedValue;
		}
	}
	
	/**
	 * Class representing the actual counters for one column family.
	 * @author Marco Scavuzzo
	 *
	 */
	private class MapVDPcounters{
		/**
		 * K - VDPid
		 * V - counter
		 */
		private volatile HashMap<Integer,Integer> vdpCounters;
		
		/**
		 * Instantiates the new HashMap containing the counter for each VDP.
		 */
		public MapVDPcounters(){
			vdpCounters = new HashMap<Integer,Integer>();
		}
		
		/**
		 * Instantiates the new HashMap (of the given size) containing the counter for each VDP.
		 */
		public MapVDPcounters(int size){
			vdpCounters = new HashMap<Integer,Integer>(size);
		}
		
		/**
		 * Atomically increments the counter for the given VDP.
		 * If it does not exist, it creates the counter and puts it to one.
		 * @param VDPid The id of the VDP whose counter should be incremented.
		 * @return the updated counter.
		 */
		public int putAndIncrement(Integer VDPid){
			synchronized(vdpCounters){
				Integer value = vdpCounters.get(VDPid);
				value = (value==null || value==0) ? 1 : value+1;
				vdpCounters.put(VDPid, value);
				return value;
			}
		}
	}
}
