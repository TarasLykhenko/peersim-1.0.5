package example.myproject;

import example.myproject.server.BackendInterface;
import peersim.config.Configuration;
import peersim.core.Linkable;
import peersim.core.Node;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {

    private static final String PAR_DEBUG = "debug";
    private static final String PAR_DEBUG_VERBOSE = "debug_verbose";
    public static final boolean DEBUG;
    /**
     * Verbose debug
     */
    public static final boolean DEBUG_V;

    public static final int DELTA;
    public static final int DELTA_MAX_SIZE;

    static {
        DEBUG = Configuration.getBoolean(PAR_DEBUG);
        DEBUG_V = Configuration.getBoolean(PAR_DEBUG_VERBOSE);
        DELTA = Configuration.getInt("delta");
        //DELTA_MAX_SIZE = (DELTA * 2) + 1;
        DELTA_MAX_SIZE = DELTA + 1; //TODO TIRAR ISTO, ISTO TA MAL!
    }

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

    public static <T> T getLastEntry(List<T> list) {
        if (list.size() == 0) {
            return null;
        }

        return list.get(list.size() - 1);
    }

    public static boolean isCrashed(Node node) {
        return nodeToBackend(node).isCrashed();
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

    public static <X, Y, Z> Map<X, Map<Y, Z>> matrixCopy(Map<X, Map<Y, Z>> matrix) {
        Map<X, Map<Y, Z>> result = new HashMap<>();

        for (X key : matrix.keySet()) {
            Map<Y, Z> line = matrix.get(key);
            Map<Y, Z> resultLine = new HashMap<>(line);
            result.put(key, resultLine);
        }

        return result;
    }

    public static <T> boolean matrixIsEqual(List<List<T>> matrixOne,
                                            List<List<T>> matrixTwo) {
        if (matrixOne.size() != matrixTwo.size()) {
            return false;
        }

        for (int i = 0; i < matrixOne.size(); i++) {
            if (matrixOne.get(i).size() != matrixTwo.get(i).size()) {
                return false;
            }

            for (int j = 0; j < matrixOne.get(i).size(); j++) {
                if (!(matrixOne.get(i).get(j).equals(matrixTwo.get(i).get(j)))) {
                    return false;
                }
            }
        }

        return true;
    }

    public static <X, Y, Z> boolean matrixIsEqual(Map<X, Map<Y, Z>> matrixOne,
                                                  Map<X, Map<Y, Z>> matrixTwo) {
        if (matrixOne.size() != matrixTwo.size()) {
            return false;
        }

        for (X key : matrixOne.keySet()) {
            if (!(matrixTwo.containsKey(key))) {
                return false;
            }

            Map<Y, Z> lineOne = matrixOne.get(key);
            Map<Y, Z> lineTwo = matrixTwo.get(key);

            if (lineOne.size() != lineTwo.size()) {
                return false;
            }

            for (Y innerKey : lineOne.keySet()) {
                if (!lineTwo.containsKey(innerKey)) {
                    return false;
                }

                Z valueOne = matrixOne.get(key).get(innerKey);
                Z valueTwo = matrixTwo.get(key).get(innerKey);

                if (valueOne != valueTwo) {
                    return false;
                }
            }
        }

        return true;
    }



}
