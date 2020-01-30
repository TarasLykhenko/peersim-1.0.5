package example.saturn.datatypes.message.types;

import static example.common.Settings.VALUE_SIZE;

public class LocalUpdateMessage extends Message{

	long value;
	int clientId;


	public LocalUpdateMessage(int key, int clientId, long value) {
		super(Type.LOCALUPDATE, key,ChannelType.TCP);
		this.clientId = clientId;
		this.value = value;
		this.setMessageSize(calculateMessageSize());

	}

	public long getValue(){
		return value;
	}

	public int getClientId(){
		return clientId;
	}

	public void setClientId(int clientId){
		this.clientId = clientId;
	}

	protected int calculateMessageSize(){
		int size = super.calculateMessageSize();
		size += VALUE_SIZE;
		size += 8; //clientId
		return size;
	}
}
