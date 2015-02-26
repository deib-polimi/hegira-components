/**
 * 
 */
package it.polimi.hegira.utils;

import java.util.HashMap;

/**
 * @author Marco Scavuzzo
 *
 */
public class VDPsCounters {

	/**
	 * K - column family name
	 * V - MapVDPcounters: counters for all VDPids relative to that cf
	 */
	private volatile HashMap<String, MapVDPcounters> cfCounters;
	
	public VDPsCounters() {
		if(cfCounters==null)
			cfCounters = new HashMap<String, MapVDPcounters>();
	}
	
	public int putAndIncrementCounter(String cf, Integer VDPid){
		synchronized(cfCounters){
			MapVDPcounters mapVDPcounters = cfCounters.get(cf);
			mapVDPcounters = (mapVDPcounters==null) ? new MapVDPcounters() : mapVDPcounters;
			int updatedValue = mapVDPcounters.putAndIncrement(VDPid);
			cfCounters.put(cf, mapVDPcounters);
			return updatedValue;
		}
	}
	
	public int putAndIncrementCounter(String cf, Integer VDPid, Integer size){
		synchronized(cfCounters){
			MapVDPcounters mapVDPcounters = cfCounters.get(cf);
			mapVDPcounters = (mapVDPcounters==null) ? new MapVDPcounters(size) : mapVDPcounters;
			int updatedValue = mapVDPcounters.putAndIncrement(VDPid);
			cfCounters.put(cf, mapVDPcounters);
			return updatedValue;
		}
	}
	
	private class MapVDPcounters{
		/**
		 * K - VDPid
		 * V - counter
		 */
		private volatile HashMap<Integer,Integer> vdpCounters;
		
		public MapVDPcounters(){
			vdpCounters = new HashMap<Integer,Integer>();
		}
		
		public MapVDPcounters(int size){
			vdpCounters = new HashMap<Integer,Integer>(size);
		}
		
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
