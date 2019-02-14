package example.oldsaturn.datatypes;


public class PendingEventUID {
	
	EventUID event;
	long senderId;	
	
	public PendingEventUID(EventUID event, long senderId){
		this.event = event;
		this.senderId = senderId;
	}
	
	public EventUID getEvent() {
		return event;
	}

	public void setEvent(EventUID event) {
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
