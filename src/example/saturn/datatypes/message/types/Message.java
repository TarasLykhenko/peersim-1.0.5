package example.saturn.datatypes.message.types;

public class Message {



	public enum Type {
		READ,
		LOCALUPDATE,
		REMOTEUPDATE,
		METADATA
	}

	public enum ChannelType {
		TCP, //FIFO
		UDP	 //not FIFO
	}

	private Type type;
	private int key;
	private long nodeOriginID; //local replica ID
	private long nodeDestinationID; //replica ID

	private ChannelType channelType;
	private int messageSize;



	public Message(Type type, int key, ChannelType channelType) {
		this.type = type;
		this.key = key;
		this.channelType = channelType;
		this.setMessageSize(calculateMessageSize());

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

	public ChannelType getChannelType() {
		return channelType;
	}
	
	public void setMessageSize(int messageSize){
		this.messageSize = messageSize;
	}

	public int getMessageSize() {
		return messageSize;
	}

	protected int calculateMessageSize(){
		int size = 0;
		size += 8; //key
		size += 8; //nodeOriginID
		size += 8; //nodeDestinationID
		size += 1; //type
		return size;
	}

}
