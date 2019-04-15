package example.common;

import example.common.datatypes.DataObject;

import java.util.Map;
import java.util.Set;

public interface GroupsManagerInterface {

    Map<Long, Map<Integer, Set<DataObject>>> getDataCenterIdsDataObjects();

    Map<Integer, Set<Long>> getExclusiveNodeToLevelNeighbourIds(long nodeId);

    default int getLowestCommonLevel(long originId, long targetId) {
        System.out.println("Origin: " + originId);
        Map<Integer, Set<Long>> originLevels = getExclusiveNodeToLevelNeighbourIds(originId);
        int level = 0;
        while (true) {
            if (originLevels.get(level) == null) {
                throw new RuntimeException("There is no common level between " + originId + " and " + targetId);
            }
            if (originLevels.get(level).contains(targetId)) {
                return level;
            }
            level++;
        }
    }
}
