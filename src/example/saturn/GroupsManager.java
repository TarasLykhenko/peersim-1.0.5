package example.saturn;

import example.common.AbstractGroupsManager;
import example.common.BasicStateTreeProtocol;
import example.common.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.core.Network;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupsManager extends AbstractGroupsManager {

    private static final String PAR_TREE = "tree";
    private static final String PAR_GROUPS = "groups";
    private final int tree;

    private Map<Long, Map<Integer, Set<DataObject>>> dataCenterIdsDataObjects = new HashMap<>();
    private Map<Long, Map<Integer, Set<Long>>> exclusiveNodeToLevelNeighbourIds;

    private Map<Long, Map<Integer, Set<StateTreeProtocol>>> nodeToLevelNeighbours = new HashMap<>();


    private Map<Integer, Set<DataObject>> levelsToAllDataObjects = new HashMap<>();
    private Map<StateTreeProtocol, Map<Integer, Set<DataObject>>> dataCenterDataObjects = new HashMap<>();

    private Map<Integer, DataObject> idToObjects = new HashMap<>();
    private Map<StateTreeProtocol, Set<DataObject>> datacenterToObjects = new HashMap<>();

    public Map<Integer, Set<DataObject>> getLevelsToAllDataObjects() {
        return levelsToAllDataObjects;
    }


    private GroupsManager() {
        tree = Configuration.getPid(PAR_TREE);
        String rawStringGroups = Configuration.getString(PAR_GROUPS);
        populate(rawStringGroups);
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
                group = group.replace("[","");
                String[] members = group.split(",");
                Set<StateTreeProtocol> datacenters = getDatacentersFromStringArray(members);
                result.put(level, datacenters);
                level++;
            }
        }
        return result;
    }

    private Set<StateTreeProtocol> getDatacentersFromStringArray(String[] members) {
        Set<StateTreeProtocol> result = new HashSet<>();

        for (String entry : members) {
            int entryInt = Integer.parseInt(entry);
            result.add((StateTreeProtocol)Network.get(entryInt).getProtocol(tree));
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

    public void addDataObjects(StateTreeProtocol datacenter, Set<DataObject> result, int level) {
        dataCenterDataObjects
                .computeIfAbsent(datacenter, k -> new HashMap<>())
                .put(level, result);
        dataCenterIdsDataObjects
                .computeIfAbsent(datacenter.getNodeId(), k -> new HashMap<>())
                .put(level, result);
        levelsToAllDataObjects
                .computeIfAbsent(level, k -> new HashSet<>())
                .addAll(result);

        for (DataObject obj : result) {
            idToObjects.put(obj.getKey(), obj);
        }
        datacenterToObjects.computeIfAbsent(datacenter, k -> new HashSet<>())
                .addAll(result);
    }

    public Set<DataObject> getDataObjectsFromDatacenter(StateTreeProtocol datacenter) {
        return datacenterToObjects.get(datacenter);
    }

    @Override
    public Map<Long, Map<Integer, Set<DataObject>>> getDataCenterIdsDataObjects() {
        return dataCenterIdsDataObjects;
    }

    @Override
    public Map<Integer, Set<Long>> getExclusiveNodeToLevelNeighbourIds(long nodeId) {
        if (exclusiveNodeToLevelNeighbourIds == null) {
            exclusiveNodeToLevelNeighbourIds = new HashMap<>();

            for (Long serverId : nodeToLevelNeighbours.keySet()) {
                Map<Integer, Set<StateTreeProtocol>> levelsAndNodes = nodeToLevelNeighbours.get(serverId);

                int level = 0;
                while (true) {
                    if (!levelsAndNodes.containsKey(level)) {
                        break;
                    }

                    Set<Long> levelNodes = levelsAndNodes.get(level).stream()
                            .map(BasicStateTreeProtocol::getNodeId)
                            .collect(Collectors.toSet());

                    // Make it exclusive
                    for (int i = level - 1; i >= 0; i--) {
                        Set<Long> existingLongs = exclusiveNodeToLevelNeighbourIds.get(serverId).get(i);
                        levelNodes.removeAll(existingLongs);
                    }

                    // Add result
                    exclusiveNodeToLevelNeighbourIds
                            .computeIfAbsent(serverId, k -> new HashMap<>())
                            .put(level, levelNodes);

                    level++;
                }
            }
        }

        return exclusiveNodeToLevelNeighbourIds.get(nodeId);
    }

    @Override
    public boolean hasBrokers() {
        return false;
    }
}
