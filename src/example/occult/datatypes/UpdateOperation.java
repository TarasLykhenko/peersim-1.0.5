package example.occult.datatypes;

import java.util.HashMap;
import java.util.Map;

public class UpdateOperation extends Operation {

	/**
	 * Mapping between data object key and versions
	 */
	Map<Integer, Integer> updateContext = new HashMap<>();

	/**
	 * We don't really write anything, since it's not necessary.
	 * @param key Key of the DataObject
	 * @param clientContext The context of the client, according to the cops paper.
	 */
	public UpdateOperation(Integer key, Map<Integer, Integer> clientContext) {
		super(Type.UPDATE, key);
		updateContext = new HashMap<>(clientContext);
	}

	public Map<Integer, Integer> getUpdateContext() {
		return updateContext;
	}

}
