package example.genericsaturn.datatypes;

import example.common.datatypes.Operation;

import static example.common.datatypes.Operation.Type.READ;

public class ReadOperation extends Operation {

	public ReadOperation(int key) {
		super(READ, key);
	}

}
