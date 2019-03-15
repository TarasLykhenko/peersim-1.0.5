package example.capstonematrix.datatypes;

import example.capstonematrix.GroupsManager;
import example.capstonematrix.StateTreeProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HRC {

    List<List<Integer>> clock = new ArrayList<>();
    private final int regionId;

    public HRC(int regionId) {
        this.regionId = regionId;
        int maxHeight = GroupsManager.getInstance().getMaxHeight();
        for (int i = 0; i <= maxHeight; i++) {
            clock.add(new ArrayList<>());
            for (int j = 0; j <= i; j++) {
                clock.get(i).add(0);
            }
        }
    }

    public HRC(HRC originalHRC) {
        List<List<Integer>> newLogMatrixClock = new ArrayList<>();
        for (List<Integer> regionList : originalHRC.clock) {
            newLogMatrixClock.add(new ArrayList<>(regionList));
        }
        this.clock = newLogMatrixClock;
        this.regionId = originalHRC.regionId;
    }

    public int getRegionId() {
        return regionId;
    }


    public HRC incorporate(int key,
                           long sourceCloudlet,
                           int updateLC) {
        HRC result = new HRC(this);

        Set<StateTreeProtocol> interestedDataCenters =
                GroupsManager.getInstance().getInterestedDatacenters(key);

        int furthestRegion = 0;
        for (StateTreeProtocol dc : interestedDataCenters) {
            long targetId = dc.getNodeId();
            int region = GroupsManager.getInstance().getFirstCommonRegion(sourceCloudlet, targetId);
            int regionHeight = GroupsManager.getInstance().getRegionHeight(region);
            if (regionHeight > furthestRegion) {
                furthestRegion = regionHeight;
            }
        }

        // Need to convert height
        int adaptedRegionHeight = revertHeight(furthestRegion);

        int lastRow = result.clock.size() - 1;
        for (int i = adaptedRegionHeight; i < result.clock.get(lastRow).size(); i++) {
            result.clock.get(lastRow).set(i, updateLC);
        }

        return result;
    }

    private int revertHeight(int normalHeight) {
        return GroupsManager.getInstance().getMaxHeight() - normalHeight;
    }

    /**
     * i = Line iterator
     * j = Column iterator
     * k = Lowest common subregion
     *
     * Transform has 4 steps:
     *
     * 1) For i < k and for any j, X:i,j remains the same
     *
     * @param nodeA
     * @param nodeB
     * @return
     */
    public HRC transform(int nodeA,
                         int nodeB) {
        HRC result = new HRC(GroupsManager.getInstance().getMostSpecificRegion(nodeB));

        int k = getK(nodeA, nodeB);

        System.out.println("k value: " + k);

        // Step 1
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < clock.get(i).size(); j++) {
                int originalValue = clock.get(i).get(j);
                result.clock.get(i).set(j, originalValue);
            }
        }

        // Step 2
        int maxValue = getMaxValue2ndStep(k);
        for (int j = 0; j < k; j++) {
            result.clock.get(k).set(j, maxValue);
        }

        // Step 3
        maxValue = getMaxValue3rdStep(k); // Different
        result.clock.get(k).set(k, maxValue);

        // Step 4, probably not necessary as the default HRC has every entry at 0
        for (int i = k + 1; i < result.clock.size(); i++) {
            for (int j = 0; j < result.clock.get(i).size(); j++) {
                result.clock.get(i).set(j, 0);
            }
        }

        return result;
    }

    private int getMaxValue2ndStep(int commonLowestSubRegion) {
        int max = 0;
        for (int l = commonLowestSubRegion; l < clock.size(); l++) {
            for (int j = 0; j < commonLowestSubRegion; j++) {
                int value = clock.get(l).get(j);
                if (value > max) {
                    max = value;
                }
            }
        }
        return max;
    }

    private int getMaxValue3rdStep(int commonLowestSubRegion) {
        int max = 0;
        for (int l = commonLowestSubRegion; l < clock.size(); l++) {
            for (int j = commonLowestSubRegion; j < clock.get(l).size(); j++) {
                int value = clock.get(l).get(j);
                if (value > max) {
                    max = value;
                }
            }
        }

        return max;
    }

    public HRC merge(HRC otherHRC) {
        if (regionId != otherHRC.regionId) {
            throw new RuntimeException("Merging incompatible HRCS!");
        }

        HRC merge = new HRC(regionId);

        for (int i = 0; i < clock.size(); i++) {
            for (int j = 0; j < clock.get(i).size(); j++) {
                int max;
                int currentEntry = clock.get(i).get(j);
                int otherEntry = otherHRC.clock.get(i).get(j);

                if (currentEntry > otherEntry) {
                    max = currentEntry;
                } else {
                    max = otherEntry;
                }

                merge.clock.get(i).set(j, max);
            }
        }

        /*
        System.out.println("Merge - HRC 1");
        print();
        System.out.println("HRC 2");
        otherHRC.print();
        System.out.println("Result");
        merge.print();
        */

        return merge;
    }

    public boolean canAcceptHRC(long sourceId, long targetId, Map<Long, Integer> lastReceived) {
        int k = getK(sourceId, targetId);
        // System.out.println("Current map: " + lastReceived);

        // We only analyze the diagonal
        for (int line = 0; line <= k; line++) {
            int value = clock.get(line).get(line);
            Set<Long> nodeIds = GroupsManager
                    .getInstance()
                    .nodesFromRelativeRegion(targetId, line);
            for (Long nodeId : nodeIds) {
                int lastReceivedValue = lastReceived.get(nodeId);
                if (value > lastReceivedValue) {
                    return false;
                }
            }
        }

        System.out.println("ACCEPTING!");
        return true;
    }

    private int getK(long sourceId, long targetId) {
        int kRegion = GroupsManager.getInstance()
                .getFirstCommonRegion(sourceId, targetId);

        int kHeight = GroupsManager.getInstance().getRegionHeight(kRegion);

        return GroupsManager.getInstance().getMaxHeight() - kHeight;
    }

    public void print() {
        int maxListSize = 0;
        for (List<Integer> regionList : clock) {
            if (regionList.size() > maxListSize) {
                maxListSize = regionList.size();
            }
        }

        for (int i = 0; i < clock.size(); i++) {
            List<Integer> regionEntries = clock.get(i);
            for (int j = 0; j < regionEntries.size(); j++) {
                System.out.print(clock.get(i).get(j) + " ");
            }

            for (int j = regionEntries.size(); j < maxListSize; j++) {
                System.out.print("- ");
            }

            System.out.println();
        }
    }


}
