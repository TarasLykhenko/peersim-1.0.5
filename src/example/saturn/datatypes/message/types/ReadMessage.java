package example.saturn.datatypes.message.types;

import static example.common.Settings.VALUE_SIZE;

public class ReadMessage extends Message{

	int clientId;

	public ReadMessage(int key, int clientId) {
		super(Type.READ, key, ChannelType.TCP);
		this.clientId = clientId;
		this.setMessageSize(calculateMessageSize());

	}

	public int getClientId(){
		return clientId;
	}

	public void setClientId(int clientId){
		this.clientId = clientId;
	}

	protected int calculateMessageSize(){
		int size = super.calculateMessageSize();
		size += 8; //clientId
		size += VALUE_SIZE;
		return size;
	}
}
