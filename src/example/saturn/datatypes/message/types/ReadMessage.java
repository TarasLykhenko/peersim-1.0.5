package example.saturn.datatypes.message.types;

public class ReadMessage extends Message{

	int clientId;

	public ReadMessage(int key, int clientId) {
		super(Type.READ, key);
		this.clientId = clientId;
	}

	public int getClientId(){
		return clientId;
	}

	public void setClientId(int clientId){
		this.clientId = clientId;
	}
}
