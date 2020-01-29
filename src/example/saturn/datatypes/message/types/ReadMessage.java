package example.saturn.datatypes.message.types;

public class ReadMessage extends Message{

	public ReadMessage(int key) {
		super(Type.READ, key);
	}
}
