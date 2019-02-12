package example.ctreeplus;

public class PendingEventUID2 {
	
	EventUID2 event;
	long senderId;
	
	
	public PendingEventUID2(EventUID2 event, long senderId){
		this.event = event;
		this.senderId = senderId;
	}
	
	public EventUID2 getEvent() {
		return event;
	}

	public void setEvent(EventUID2 event) {
		this.event = event;
	}

	public long getSenderId() {
		return senderId;
	}

	public void setSenderId(long senderId) {
		this.senderId = senderId;
	}
	
	public String toString() {
		return event.toString()+". Sent by "+senderId;
	}
	
}
