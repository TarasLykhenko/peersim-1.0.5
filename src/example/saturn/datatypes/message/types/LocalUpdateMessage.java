package example.saturn.datatypes.message.types;

public class LocalUpdateMessage extends Message{

	long value;
	int clientId;


	public LocalUpdateMessage(int key, int clientId, long value) {
		super(Type.LOCALUPDATE, key);
		this.clientId = clientId;
		this.value = value;
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
}
