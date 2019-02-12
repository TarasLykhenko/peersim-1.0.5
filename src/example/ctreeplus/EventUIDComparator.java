package example.ctreeplus;

import java.util.Comparator;

public class EventUIDComparator implements Comparator<EventUID> {

	@Override
	public int compare(EventUID e1, EventUID e2) {
		if (e1.getTimestamp() < e2.getTimestamp()){
			return -1;
		}else if(e1.getTimestamp() > e2.getTimestamp()){
			return 1;
		}
		return 0;
	}
}
