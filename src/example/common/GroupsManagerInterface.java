package example.common;

import example.common.datatypes.DataObject;

import java.util.Map;
import java.util.Set;

public interface GroupsManagerInterface {

    Map<Long, Map<Integer, Set<DataObject>>> getDataCenterIdsDataObjects();

    Map<Integer, Set<Long>> getExclusiveNodeToLevelNeighbourIds(long nodeId);

    int getLowestCommonLevel(long originId, long targetId);

    boolean hasBrokers();
}
