package example.saturn.datatypes;

import java.util.HashMap;

public class VectorMessage {
	
	final EventUID event;
	final HashMap<String, HashMap<Integer, Integer>> vector;
	final int epoch;
	public VectorMessage( EventUID event, HashMap<String, HashMap<Integer, Integer>> vector, int epoch)
	{
		this.event = event;
		this.vector = vector;
		this.epoch = epoch;
	}
	public EventUID getEvent() {
		return event;
	}
	public HashMap<String, HashMap<Integer, Integer>> getVector() {
		return vector;
	}
	public int getEpoch() {
		return epoch;
	}
	
	public VectorMessage clone(){
		EventUID e = new EventUID(event.getKey(), event.getTimestamp(), event.getEpoch(), event.getRemoteRead(), event.getLatency(), event.getSrc(), event.getDst());
		HashMap<String, HashMap<Integer, Integer>> v = new HashMap<String, HashMap<Integer, Integer>>();
		for (String key : vector.keySet()){
			HashMap<Integer, Integer> array = v.get(key);
			HashMap<Integer, Integer> newArray = new HashMap<Integer, Integer>();
			for (Integer server : array.keySet()){
				Integer value = array.get(server);
				newArray.put(new Integer(server), new Integer(value));
			}
			v.put(new String(key), newArray);
		}
		return new VectorMessage(e, v, epoch);
	}
}
