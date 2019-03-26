package example.myproject;

import example.myproject.server.BackendInterface;
import peersim.config.Configuration;
import peersim.core.Linkable;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {

    private Utils() {}

    private static final int BACKEND_PID = Configuration.getPid("causalneighbour");
    private static final int LINKABLE_PID = Configuration.getPid("linkable");

    public static Set<Node> getNeighboursExcludingSource(Node currentNode, Node sourceNode) {
        Linkable linkable = (Linkable)
                currentNode.getProtocol(LINKABLE_PID);
        Set<Node> neighbours = new HashSet<>();
        for (int i = 0; i < linkable.degree(); i++) {
            neighbours.add(linkable.getNeighbor(i));
        }
        neighbours.remove(sourceNode);

        return neighbours;
    }

    public static boolean isCrashed(Node node) {
        return !node.isUp();
    }

    public static BackendInterface nodeToBackend(Node node) {
        return (BackendInterface) node.getProtocol(BACKEND_PID);
    }

    public static Set<Long> nodesToLongs(Set<Node> nodes) {
        return nodes.stream().map(Node::getID).collect(Collectors.toSet());
    }

    public static <T> List<List<T>> matrixCopy(List<List<T>> matrix) {
        List<List<T>> result = new ArrayList<>();

        for (List<T> vector : matrix) {
            List<T> resultVector = new ArrayList<>(vector);
            result.add(resultVector);
        }

        return result;
    }

}
