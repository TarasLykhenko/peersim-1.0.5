package example.ctreeplus;

public class EventUID {
	
	long nodeid;

	int timestamp;
	
	public EventUID(long nodeid, int timestamp){
		this.nodeid = nodeid;
		this.timestamp = timestamp;
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
	
	public String toString() {
		return "<"+nodeid+", "+timestamp+">";
	}
	public String toStringFileFormat() {
		return nodeid+","+timestamp;
	}
	
}
