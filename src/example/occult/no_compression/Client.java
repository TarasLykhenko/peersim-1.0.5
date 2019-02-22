package example.occult.no_compression;

import example.common.datatypes.DataObject;
import example.occult.OccultClient;
import example.occult.StateTreeProtocol;
import peersim.core.CommonState;

import java.util.Map;
import java.util.Set;

public class Client extends OccultClient {

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);
    }


    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    @Override
    public int getShardStampFromCS(int shardId) {
        return clientTimestamp.getOrDefault(shardId, 0);
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
    public void occultReceiveUpdateResult(Integer shardId, Integer updateShardStamp) {
        int oldShardStamp = clientTimestamp.getOrDefault(shardId, 0);
        if (updateShardStamp > oldShardStamp) {
            clientTimestamp.put(shardId, updateShardStamp);
        }

   //     System.out.println("Received update!");
        isWaitingForResult = false;
        lastResultReceivedTimestamp = CommonState.getTime();
        updatesTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);
    }

}
