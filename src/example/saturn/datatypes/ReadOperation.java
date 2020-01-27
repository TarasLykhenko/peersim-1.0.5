package example.saturn.datatypes;

import example.common.datatypes.Operation;

public class ReadOperation extends Operation {

	public ReadOperation(int key) {
		super(Type.READ, key);
	}
}
