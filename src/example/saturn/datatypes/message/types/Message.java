package example.saturn.datatypes.message.types;

import example.common.datatypes.Operation;

public class Message {

	public Message(int key) {
		super(Type.READ, key);
	}
}
