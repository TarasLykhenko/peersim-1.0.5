package example.occult.temporal_compression;

import example.common.datatypes.DataObject;
import example.occult.GroupsManager;
import example.occult.OccultClient;
import example.occult.StateTreeProtocol;
import peersim.config.Configuration;
import peersim.core.CommonState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Client extends OccultClient {

    private static final String PAR_CLIENT_TS_SIZE = "client_ts_size";

    private final int maxCtsSize;

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality, GroupsManager groupsManager) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality, groupsManager);
        this.maxCtsSize = Configuration.getInt(PAR_CLIENT_TS_SIZE);
    }

    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    @Override
    public int getShardStampFromCS(int shardId) {
        if (clientTimestamp.containsKey(shardId)) {
            return clientTimestamp.get(shardId);
        } else if (clientTimestamp.size() < maxCtsSize) {
            // Special case: Still filling the ClientTimestamp:
            return 0;
        } else {
            totalCatchAllReads++;
            return catchAllShardStamp;
        }
    }

    @Override
    public void occultReceiveUpdateResult(Integer shardId, Integer updateShardStamp) {
        updateClientTimeStamp(shardId, updateShardStamp);

        //     System.out.println("Received update!");
        isWaitingForResult = false;
        lastResultReceivedTimestamp = CommonState.getTime();
        updatesTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);
    }

    private void updateClientTimeStamp(Integer shardId, Integer updateShardStamp) {
        // Scenario 1: Client timestamp contains the entry, check if update
        // System.out.println("My ts: " + clientTimestamp);
        // System.out.println("ShardID: " + shardId + " | updateStamp: " + updateShardStamp);
        if (clientTimestamp.containsKey(shardId)) {
            int oldShardStamp = clientTimestamp.get(shardId);
            //System.out.println("Scenario 1 Updating...(" + oldShardStamp + " - " + updateShardStamp);
            if (updateShardStamp > oldShardStamp) {
                //    System.out.println("Updated!");
                clientTimestamp.put(shardId, updateShardStamp);
            }
        } else if (clientTimestamp.size() < maxCtsSize) {
            // System.out.println("Adding..");
            // Scenario 2: Client TS does not contain the entry but
            // has space for more entries, therefore add it
            //  System.out.println("Scenario 2: My Size: " + clientTimestamp.size() + " | maxSize: " + maxCtsSize);
            clientTimestamp.put(shardId, updateShardStamp);
        } else {
            // System.out.println("Adding to catch all. (MySize: " + clientTimestamp.size() + ")");
            // Scenario 3: ShardID not explicitly tracked, add Catchall
            int lowestShardId = 0;
            int lowestShardIdStamp = Integer.MAX_VALUE;
            for (Integer trackedShardId : clientTimestamp.keySet()) {
                if (clientTimestamp.get(trackedShardId) < lowestShardIdStamp) {
                    lowestShardIdStamp = clientTimestamp.get(trackedShardId);
                    lowestShardId = trackedShardId;
                }
            }

            clientTimestamp.remove(lowestShardId);
            if (lowestShardIdStamp < catchAllShardStamp) {
                catchAllShardStamp = lowestShardIdStamp;
            }
        }
        if (clientTimestamp.size() > maxCtsSize) {
            // System.out.println("My ts is " + clientTimestamp);
            throw new RuntimeException("update failed");
        }
    }

    @Override
    public void updateClientTimestampFromRead(
            Map<Integer, Integer> timeStamp1, int catchAllOne,
            Map<Integer, Integer> timestamp2, int catchAllTwo) {
        // We start the merge by copying the first map.
        Map<Integer, Integer> result = new HashMap<>(timeStamp1);
        //System.out.println("M1: " + timeStamp1);
        //System.out.println("M2: " + timestamp2);
        //System.out.println("CatchAll 1:" + catchAllOne);
        //System.out.println("Catch all 2:" + catchAllTwo);
        int highestCatchAll;
        if (catchAllOne < catchAllTwo) {
            highestCatchAll = catchAllTwo;
        } else {
            highestCatchAll = catchAllOne;
        }
        //System.out.println("Highest catchAll: " + highestCatchAll);

        for (Integer shardIdTwo : timestamp2.keySet()) {
            int shardStampTwo = timestamp2.get(shardIdTwo);

            // Case 1: Both maps contain the same shardIdTwo, update
            if (result.containsKey(shardIdTwo) && result.get(shardIdTwo) < shardStampTwo) {
                result.put(shardIdTwo, shardStampTwo);
            } else if (!result.containsKey(shardIdTwo) && result.size() < maxCtsSize) {
                // Case 2: Client Timestamp does not contain sameKey
                // but can have more entries, add to result
                result.put(shardIdTwo, shardStampTwo);
            } else if (result.size() == maxCtsSize) {
                // Case 3: Map 2 contains a key that Map 1 does not, check if
                // its value is higher and the result is already filled
                for (Integer resultShardId : result.keySet()) {
                    int resultShardStamp = result.get(resultShardId);
                    if (resultShardStamp < shardStampTwo) {
                        result.remove(resultShardId);
                        result.put(shardIdTwo, shardStampTwo);
                        if (resultShardStamp > highestCatchAll) {
                            //   System.out.println("Updating catchall");
                            highestCatchAll = resultShardStamp;
                        }
                        break;
                    }
                }
            }
        }
        if (result.size() > maxCtsSize) {
            System.out.println("My ts size is " + result.size() + " | " + maxCtsSize);
            throw new RuntimeException("Merge failed");
        }

        //System.out.println("Merge: " + result);
        //System.out.println("Catch all result: " + highestCatchAll);
        this.catchAllShardStamp = highestCatchAll;
        this.clientTimestamp = result;
    }
}
