package example.cops.datatypes;

import java.util.Map;

public class UpdateOperation extends Operation {

	private final Long shardMasterNodeId;
	private final Long clientId;
	private final Map<Integer, Integer> clientTimestamp;

	/**
	 * We don't really write anything, so we don't provide any value.
	 * @param shardMasterNodeId Datacenter that contains the shard that is the master
	 * @param key Key of the DataObject
	 * @param clientId identifier of the update, so that the datacenter can respond to.
	 * @param clientTimestamp The causalTimestamp of the client, according to Occult
	 */
	public UpdateOperation(Long shardMasterNodeId,
						   Integer key,
						   Long clientId,
						   Map<Integer, Integer> clientTimestamp) {
		super(Type.UPDATE, key);
		this.shardMasterNodeId = shardMasterNodeId;
		this.clientId = clientId;
		this.clientTimestamp = clientTimestamp;
	}

	public Long getShardMasterNodeId() {
		return shardMasterNodeId;
	}

	public Long getClientId() {
		return clientId;
	}

	public Map<Integer, Integer> getClientTimestamp() {
		return clientTimestamp;
	}

	public Map<Integer, Integer> getUpdateContext() {
		return updateContext;
	}

}
