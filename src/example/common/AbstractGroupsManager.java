package example.common;

import peersim.core.Network;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractGroupsManager implements GroupsManagerInterface {

    private Map<Long, Map<Long, Integer>> commonLevelTable;

    public int getLowestCommonLevel(long originId, long targetId) {
        if (commonLevelTable == null) {
            commonLevelTable = new HashMap<>();

            for (long source = 0; source < Network.size(); source++) {
                Map<Integer, Set<Long>> targetsAndLevels = getExclusiveNodeToLevelNeighbourIds(source);
                for (int level : targetsAndLevels.keySet()) {
                    Set<Long> targets = targetsAndLevels.get(level);
                    for (long target : targets) {
                        commonLevelTable.computeIfAbsent(source, k -> new HashMap<>()).put(target, level);
                    }
                }
            }
        }

        return commonLevelTable.get(originId).get(targetId);
    }
}
