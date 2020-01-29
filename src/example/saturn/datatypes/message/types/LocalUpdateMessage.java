package example.saturn.datatypes.message.types;

public class LocalUpdateMessage extends Message{

	public LocalUpdateMessage(int key) {
		super(Type.LOCALUPDATE, key);
	}
}
