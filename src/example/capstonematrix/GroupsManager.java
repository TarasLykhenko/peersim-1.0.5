package example.capstonematrix;

import example.common.datatypes.DataObject;
import org.omg.SendingContext.RunTime;
import peersim.config.Configuration;
import peersim.core.Network;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupsManager {

    private static final String PAR_TREE = "tree";
    private static final String PAR_GROUPS = "groups";
    private final int tree;

    private Map<Long, Map<Integer, Set<StateTreeProtocol>>> nodeToLevelNeighbours = new HashMap<>();

    private Map<Integer, Set<DataObject>> levelsToAllDataObjects = new HashMap<>();
    private Map<StateTreeProtocol, Map<Integer, Set<DataObject>>> dataCenterDataObjects = new HashMap<>();

    private Map<Integer, DataObject> idToObjects = new HashMap<>();
    private Map<Long, StateTreeProtocol> idToStateTreeProtocol = new HashMap<>();
    private Map<StateTreeProtocol, Set<DataObject>> datacenterToObjects = new HashMap<>();


    private int MAX_HEIGHT;
    private Map<Set<StateTreeProtocol>, Integer> nodesToRegion = new HashMap<>();
    private Map<Integer, Set<StateTreeProtocol>> regionToNodes = new HashMap<>();
    private Map<Long, List<Integer>> nodeToRegionList = new HashMap<>();
    private Map<Integer, Integer> regionHeight = new HashMap<>();

    private Map<Integer, Set<StateTreeProtocol>> keysToDatacenters = new HashMap<>();


    private GroupsManager() {
        tree = Configuration.getPid(PAR_TREE);
        String rawStringGroups = Configuration.getString(PAR_GROUPS);
        populate(rawStringGroups);
        //debugRegions();
    }

    private void debugRegions() {
        for (Map.Entry<Set<StateTreeProtocol>, Integer> entry : nodesToRegion.entrySet()) {
            System.out.println("Region nodes: " + entry.getKey() + " | regionId: " + entry.getValue());
        }

        for (Map.Entry<Integer, Set<StateTreeProtocol>> entry : regionToNodes.entrySet()) {
            System.out.println("RegionId: " + entry.getKey() + " | regionNodes: " + entry.getValue());
        }

        for (Map.Entry<Integer, Integer> entry : regionHeight.entrySet()) {
            int regionId = entry.getKey();
            int height = entry.getValue();

            System.out.println("Region: " + regionId + " | Height: " + height);
        }

        for (Map.Entry<Long, List<Integer>> entry : nodeToRegionList.entrySet()) {
            long nodeId = entry.getKey();
            List<Integer> regionIdList = entry.getValue();
            System.out.print("Node " + nodeId + " regions: ");
            for (Integer regionId : regionIdList) {
                System.out.print(regionToNodes.get(regionId));
            }
            System.out.println();
        }
    }

    private static GroupsManager groupsManager;


    public static GroupsManager getInstance() {
        if (groupsManager == null) {
            groupsManager = new GroupsManager();
        }
        return groupsManager;
    }


    // All datacenters have all to all connection
    // Custom parsing a string ._.
    // Format: { : starts a specific node info
    //         } : ends a specific node info
    //         [ : defines the start of a group
    //         ] : defines the end of a group
    private int currentCounter = 0;

    public void populate(String rawStringGroups) {
        char[] groupInfoChars = rawStringGroups.toCharArray();
        long currentNode = 0;

        for (; currentCounter < groupInfoChars.length; currentCounter++) {
            if (groupInfoChars[currentCounter] == '{') {

                Map<Integer, Set<StateTreeProtocol>> nodeGroups = parseNodeInfo(groupInfoChars);
                nodeToLevelNeighbours.put(currentNode, nodeGroups);
                currentNode++;
            }
        }

        for (long i = 0; i < Network.size(); i++) {
            idToStateTreeProtocol.put(i, (StateTreeProtocol) Network.get((int) i).getProtocol(tree));
        }

        System.out.println();
    }

    private Map<Integer, Set<StateTreeProtocol>> parseNodeInfo(char[] groupInfoChars) {
        Map<Integer, Set<StateTreeProtocol>> result = new HashMap<>();
        int start = 0;
        int level = 0;
        while (groupInfoChars[currentCounter] != '}') {
            currentCounter++;
            if (groupInfoChars[currentCounter] == '[') {
                start = currentCounter;
            }
            if (groupInfoChars[currentCounter] == ']') {
                String group = new String(groupInfoChars, start, (currentCounter - start));
                group = group.replace("[", "");
                String[] members = group.split(",");
                Set<StateTreeProtocol> datacenters = getDatacentersFromStringArray(members);
                result.put(level, datacenters);
                addRegion(datacenters);
                level++;
            }
        }
        return result;
    }

    private int HARDCODED_LOG2_CONVERTER(int i) {
        if (i == 1) {
            return 0;
        } else if (i == 2) {
            return 1;
        } else if (i == 4) {
            return 2;
        } else if (i == 8) {
            return 3;
        } else {
            throw new RuntimeException("Unknown log result /shrug");
        }
    }

    private int regionCounter = 100_000;

    private void addRegion(Set<StateTreeProtocol> datacenters) {
        if (nodesToRegion.containsKey(datacenters)) {
            //System.out.println("Region with " + datacenters + " already exists");
            return;
        } else {
            regionCounter++;
            Set<StateTreeProtocol> key = new HashSet<>(datacenters);
            nodesToRegion.put(key, regionCounter);
            regionToNodes.put(regionCounter, key);
            int height = HARDCODED_LOG2_CONVERTER(datacenters.size());
            regionHeight.put(regionCounter, height);
            if (height > MAX_HEIGHT) {
                MAX_HEIGHT = height;
            }
        }
        int thisRegionSize = datacenters.size();
        for (StateTreeProtocol dc : datacenters) {
            List<Integer> regionIds =
                    nodeToRegionList.computeIfAbsent(dc.getNodeId(), k -> new ArrayList<>());

            boolean changed = false;
            for (int i = 0; i < regionIds.size(); i++) {
                int iterRegionSize = regionToNodes.get(regionIds.get(i)).size();
                if (thisRegionSize < iterRegionSize) {
                    regionIds.add(i, regionCounter);
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                regionIds.add(regionCounter);
            }
        }
    }

    public int getMaxHeight() {
        return MAX_HEIGHT;
    }

    public int getFirstCommonRegion(long sourceId, long targetId) {
        List<Integer> sourceRegions = nodeToRegionList.get(sourceId);
        List<Integer> targetRegions = nodeToRegionList.get(targetId);

        int firstCommonRegion = -1;
        for (int i = 0; i < sourceRegions.size(); i++) {
            int sourceRegionId = sourceRegions.get(i);
            int targetRegionId = targetRegions.get(i);

            if (sourceRegionId == targetRegionId) {
                firstCommonRegion = sourceRegionId;
                break;
            }
        }

       // System.out.println("First common region between " + sourceId + " and " + targetId +
       //         " is " + regionToNodes.get(firstCommonRegion));

        return firstCommonRegion;
    }

    public int getRegionHeight(int regionId) {
        return regionHeight.get(regionId);
    }

    private Set<StateTreeProtocol> getDatacentersFromStringArray(String[] members) {
        Set<StateTreeProtocol> result = new HashSet<>();

        for (String entry : members) {
            int entryInt = Integer.parseInt(entry);
            result.add((StateTreeProtocol) Network.get(entryInt).getProtocol(tree));
        }

        return result;
    }

    public Map<Integer, Set<StateTreeProtocol>> getNeighboursOfDatacenter(Long nodeId) {
        // debugPrintNodeGroups(nodeId);

        return nodeToLevelNeighbours.get(nodeId);
    }

    private void debugPrintNodeGroups(Long nodeId) {
        Map<Integer, Set<StateTreeProtocol>> nodeLevels = nodeToLevelNeighbours.get(nodeId);
        System.out.println("I am node " + nodeId + ". I have ");
        for (Integer level : nodeLevels.keySet()) {
            String nodes = nodeLevels.get(level).stream()
                    .map(StateTreeProtocol::getNodeId)
                    .map(Object::toString)
                    .collect(Collectors.joining(" "));

            System.out.println("  lvl " + level + " - " + nodes);
        }
    }

    public Set<StateTreeProtocol> getInterestedDatacenters(int key) {
        return keysToDatacenters.get(key);
    }

    public void addDataObjects(StateTreeProtocol datacenter, Set<DataObject> result, int level) {
        dataCenterDataObjects
                .computeIfAbsent(datacenter, k -> new HashMap<>())
                .put(level, result);
        levelsToAllDataObjects
                .computeIfAbsent(level, k -> new HashSet<>())
                .addAll(result);

        for (DataObject obj : result) {
            idToObjects.put(obj.getKey(), obj);
        }
        datacenterToObjects.computeIfAbsent(datacenter, k -> new HashSet<>())
                .addAll(result);

        for (DataObject dataObject : result) {
            keysToDatacenters.computeIfAbsent(dataObject.getKey(), k -> new HashSet<>())
                    .add(datacenter);
        }
    }

    public Map<Integer, Set<DataObject>> getLevelsToAllDataObjects() {
        return levelsToAllDataObjects;
    }

    public Set<DataObject> getDataObjectsFromDatacenter(StateTreeProtocol datacenter) {
        return datacenterToObjects.get(datacenter);
    }

    public int getMostSpecificRegion(long nodeId) {
        return nodeToRegionList.get(nodeId).get(0);
    }

    public Set<Long> nodesFromRelativeRegion(long targetId, int line) {
        int reverseValue = MAX_HEIGHT - line;
        int regionId = nodeToRegionList.get(targetId).get(reverseValue);
        return regionToNodes.get(regionId)
                .stream()
                .map(StateTreeProtocol::getNodeId)
                .collect(Collectors.toSet());
    }
}
