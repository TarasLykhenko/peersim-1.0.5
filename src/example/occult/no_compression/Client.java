package example.occult.no_compression;

import example.common.datatypes.DataObject;
import example.occult.GroupsManager;
import example.occult.OccultClient;
import example.occult.StateTreeProtocol;
import peersim.core.CommonState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class Client extends OccultClient {

    Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);

        for (DataObject dataObject : possibleDataObjects) {
            int shardId = GroupsManager.getInstance().getShardId(dataObject.getKey());
            clientTimestamp.put(shardId, 0);
        }
    }


    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    @Override
    public int getShardStampFromCS(int shardId) {
        return clientTimestamp.get(shardId);
    }

    @Override
    public void updateClientTimestampFromRead(Map<Integer, Integer> clientTimestamp,
                                              int clientCatchAll,
                                              Map<Integer, Integer> deps,
                                              int depsCatchAll) {
        for (Integer shardId : clientTimestamp.keySet()) {
            Integer clientVersion = clientTimestamp.get(shardId);
            Integer depsVersion = deps.get(shardId);

            if (depsVersion == null) {
                continue;
            }

            if (depsVersion > clientVersion) {
                this.clientTimestamp.put(shardId, depsVersion);
            }
        }
    }

    @Override
    public void receiveUpdateResult(Integer shardId, Integer updateShardStamp) {
        int oldShardStamp = clientTimestamp.get(shardId);
        if (updateShardStamp > oldShardStamp) {
            clientTimestamp.put(shardId, updateShardStamp);
        }

   //     System.out.println("Received update!");
        isWaitingForResult = false;
        lastResultReceivedTimestamp = CommonState.getTime();
        numberUpdates++;
        updatesTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);
    }

    @Override
    public Client clone() {
        return new Client(
                id,
                isEager,
                new HashMap<>(dataObjectsPerLevel), datacenter, locality);
    }


}
