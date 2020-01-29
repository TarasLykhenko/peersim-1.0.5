package example.saturn.datatypes;

import example.common.datatypes.Operation;

import java.util.HashMap;
import java.util.Map;

public class UpdateOperation extends Operation {

	/**
	 * Mapping between data object key and versions
	 */
	Map<Integer, Integer> updateContext = new HashMap<>();

	long value;

	/**
	 * We don't really write anything, since it's not necessary.
	 * @param key Key of the DataObject
	 * @param clientContext The context of the client, according to the cops paper.
	 */
	public UpdateOperation(Integer key, Map<Integer, Integer> clientContext) {
		super(Type.UPDATE, key);
		updateContext = new HashMap<>(clientContext);
	}

	public UpdateOperation(Integer key, long value) {
		super(Type.UPDATE, key);
		this.value = value;
	}


	public Map<Integer, Integer> getUpdateContext() {
		return updateContext;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

}
