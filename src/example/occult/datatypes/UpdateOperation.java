package example.occult.datatypes;

import example.common.datatypes.Operation;

import java.util.HashMap;
import java.util.Map;

public class UpdateOperation extends Operation {

	/**
	 * Mapping between data object key and versions
	 */
	final Map<Integer, Integer> updateContext;
	final int catchAll;

	/**
	 * We don't really write anything, since it's not necessary.
	 * @param key Key of the DataObject
	 * @param deps The context of the client, according to the cops paper.
	 */
	public UpdateOperation(Integer key, Map<Integer, Integer> deps, int catchAll) {
		super(Type.UPDATE, key);
		updateContext = new HashMap<>(deps);
		this.catchAll = catchAll;
	}

	public Map<Integer, Integer> getDeps() {
		return updateContext;
	}

	public int getCatchAll() {
		return catchAll;
	}

}
