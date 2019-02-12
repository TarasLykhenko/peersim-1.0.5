package example.ctreeplus;

import java.util.Comparator;

public class PendingEventUIDComparator implements Comparator<PendingEventUID> {

	@Override
	public int compare(PendingEventUID p1, PendingEventUID p2) {
		EventUID e1 = p1.getEvent();
		EventUID e2 = p1.getEvent();
		if (e1.getTimestamp() < e2.getTimestamp()){
			return -1;
		}else if(e1.getTimestamp() > e2.getTimestamp()){
			return 1;
		}
		return 0;
	}
}
