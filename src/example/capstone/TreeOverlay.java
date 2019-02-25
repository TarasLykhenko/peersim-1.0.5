package example.capstone;

import peersim.graph.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class TreeOverlay {

    private long root;
    private Map<Long, Node> nodeIdToNodes = new HashMap<>();

    TreeOverlay(Graph graph, long root) {
        this.root = root;
        Collection<Long> children = intToLong(graph.getNeighbours(Math.toIntExact(root)));
        Node parent = addNode(root, null, children);
        for (Long child : children) {
            recursiveGenerateChildren(graph, child, parent);
        }
    }

    private Collection<Long> intToLong(Collection<Integer> collection) {
        return collection.stream().map(Integer::longValue).collect(Collectors.toSet());
    }

    private void recursiveGenerateChildren(Graph graph, long nodeId, Node parent) {
        Collection<Long> neighbours = intToLong(graph.getNeighbours(Math.toIntExact(nodeId)));
        neighbours.remove(parent.nodeId);
        Node newNode = addNode(nodeId, parent, neighbours);
        for (Long child : neighbours) {
            if (ProtocolMapperInit.nodeType.get(child) == ProtocolMapperInit.Type.BROKER) {
                recursiveGenerateChildren(graph, child, newNode);
            } else {
                addNode(child, newNode, Collections.emptyList());
            }
        }
    }

    private Node addNode(long nodeId, Node parent, Collection<Long> children) {
        Node newNode = new Node(nodeId, parent, children);
        nodeIdToNodes.put(nodeId, newNode);
        return newNode;
    }

    private void debug() {
        Node rootNode = nodeIdToNodes.get(root);
        System.out.println("I am root: " + rootNode.nodeId);
        for (Long child : rootNode.children) {
            debugChild(child);
        }
    }

    private void debugChild(Long node) {
        System.out.println(node + " has parent " + nodeIdToNodes.get(node).parent.nodeId);
        for (Long child : nodeIdToNodes.get(node).children) {
            debugChild(child);
        }
    }

    List<Long> getNodesOnPathToRoot(long nodeId) {
        List<Long> result = new ArrayList<>();
        result.add(nodeId);
        Node node = nodeIdToNodes.get(nodeId);
        while (node.parent != null) {
            node = node.parent;
            result.add(node.nodeId);
        }

        return result;
    }

    Set<Long> getLeaves() {
        Set<Long> result = new HashSet<>();
        for (Long nodeId : nodeIdToNodes.keySet()) {
            if (nodeIdToNodes.get(nodeId).children.isEmpty()) {
                result.add(nodeId);
            }
        }
        return result;
    }

    Long getParent(long srcNode) {
        Node parent = nodeIdToNodes.get(srcNode).parent;
        if (parent == null) {
            return null;
        } else {
            return parent.nodeId;
        }
    }

    Collection<Long> getChildren(long srcNode) {
        return nodeIdToNodes.get(srcNode).children;
    }

    boolean nodeIsParent(long srcId, long targetId) {
        Node srcNode = nodeIdToNodes.get(srcId);
        if (srcNode.parent != null) {
            return srcNode.parent.nodeId == targetId;
        } else {
            return false;
        }
    }

    class Node {

        final long nodeId;
        final Node parent;
        final Collection<Long> children;

        Node(long nodeId, Node parent, Collection<Long> children) {
            this.nodeId = nodeId;
            this.parent = parent;
            this.children = children;
        }
    }
}
