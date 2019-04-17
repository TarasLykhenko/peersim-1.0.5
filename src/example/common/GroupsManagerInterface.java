package example.common;

import example.common.datatypes.DataObject;

import java.util.Map;
import java.util.Set;

public interface GroupsManagerInterface {

    Map<Long, Map<Integer, Set<DataObject>>> getDataCenterIdsDataObjects();

    Map<Integer, Set<Long>> getExclusiveNodeToLevelNeighbourIds(long nodeId);

    default int getLowestCommonLevel(long originId, long targetId) {
        Map<Integer, Set<Long>> originLevels = getExclusiveNodeToLevelNeighbourIds(originId);
        int level = 0;
        int maxLevel = originLevels.keySet().stream().max(Integer::compareTo).orElse(0) + 1;

        while (level < maxLevel) {
            if (originLevels.get(level) != null && originLevels.get(level).contains(targetId)) {
                return level;
            }
            level++;
        }

        // There's no good way to do this. We use exclusiveNodeToLevelNeighbours here,
        // however this is also used on the client code to get a server to migrate to.
        // If we had the brokers to this map then the broker becomes an available target
        // to migrate to, which is wrong, therefore the connection between broker and DC won't
        // be added to the map. If a connection between origin and DC does not exist then
        // it must be between a DC and a broker, which is level 1.
        if (Settings.PRINT_VERBOSE) {
            System.out.println("Connection between " + originId + " and " + targetId + " not found. Must be a DC and broker.");
        }
        return 1;
        //throw new NullPointerException("No common level found between " + originId + " and " + targetId);
    }
}
