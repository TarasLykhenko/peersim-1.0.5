package example.saturn.datatypes;

import java.util.HashMap;
import java.util.Vector;


public class VersionVector {
	
	HashMap<Long, Integer> vector;

	public VersionVector(){
		vector = new HashMap<Long, Integer>();
	}
	
	public VersionVector(VersionVector vv){
		HashMap<Long, Integer> init = vv.getVector();
		vector = new HashMap<Long, Integer>();
		for (Long key : init.keySet()){
			vector.put(key, vector.get(key));
		}
	}
	/** Return
	 * @param id
	 * @param dot
	 * @return
	 * 			0: Added
	 * 			1: Seen
	 */
	public int addEvent(long id, int dot){
		if (vector.containsKey(id)){
			Integer Counter = vector.get(id);
			if (Counter >= dot){
				return 1;
			}else{
				vector.put(id, dot);
				return 0;
			}
		}else{
			vector.put(id, dot);
			return 0;
		}
	}
	
	public boolean seenEvent(long id, int dot){
		if (vector.containsKey(id)){
			Integer Counter = vector.get(id);
			if (Counter >= dot){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}
	
	public HashMap<Long, Integer> getVector(){
		return vector;
	}
	
	public String toString(){
		return vector.toString();
	}
}
