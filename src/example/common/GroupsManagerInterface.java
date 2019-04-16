package example.common;

import example.common.datatypes.DataObject;

import java.util.Map;
import java.util.Set;

public interface GroupsManagerInterface {

    Map<Long, Map<Integer, Set<DataObject>>> getDataCenterIdsDataObjects();

    Map<Integer, Set<Long>> getExclusiveNodeToLevelNeighbourIds(long nodeId);

    default int getLowestCommonLevel(long originId, long targetId) {
        Map<Integer, Set<Long>> originLevels = getExclusiveNodeToLevelNeighbourIds(originId);
        if (originLevels == null) {
            // Brokers
            return -1;
        }
        int level = 0;
        while (true) {
            if (originLevels.get(level) == null) {
                // This is between a broker and a DC or between two brokers
                return -1;
                // throw new RuntimeException("There is no common level between " + originId + " and " + targetId);
            }
            if (originLevels.get(level).contains(targetId)) {
                return level;
            }
            level++;
        }
    }
}
