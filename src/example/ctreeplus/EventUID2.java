package example.ctreeplus;

public class EventUID2 {
	
	long nodeid;
	int epoch;
	int timestamp;
	
	public EventUID2(long nodeid, int timestamp, int epoch){
		this.nodeid = nodeid;
		this.timestamp = timestamp;
		this.epoch = epoch;
	}
	
	public long getNodeid() {
		return nodeid;
	}

	public void setNodeid(long nodeid) {
		this.nodeid = nodeid;
	}

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}
	
	public int getEpoch() {
		return epoch;
	}

	public void setEpoch(int epoch) {
		this.epoch = epoch;
	}
	
	public String toString() {
		return "<"+nodeid+", "+timestamp+">";
	}
	public String toStringFileFormat() {
		return nodeid+","+timestamp;
	}
	
}
