package example.saturn.datatypes.message.types;

public class Message {

	public enum Type {
		READ,
		LOCALUPDATE,
		REMOTEUPDATE,
		METADATA
	}

	private Type type;
	private int key;
	private long nodeOriginID; //local replica ID
	private long nodeDestinationID; //replica ID


	public Message(Type type, int key) {
		this.type = type;
		this.key = key;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public void setNodeOriginID(long nodeOriginID) {
		this.nodeOriginID = nodeOriginID;
	}

	public long getNodeOriginID() {
		return this.nodeOriginID;
	}

	public void setNodeDestinationID(long nodeDestinationID) {
		this.nodeDestinationID = nodeDestinationID;
	}

	public long getNodeDestinationID() {
		return this.nodeDestinationID;
	}

}
