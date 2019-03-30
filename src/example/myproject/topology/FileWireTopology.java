package example.myproject.topology;

import example.myproject.ScenarioReader;
import example.myproject.datatypes.AssertException;
import javafx.util.Pair;
import peersim.core.Network;
import peersim.graph.Graph;

import java.util.HashSet;
import java.util.Set;

public class FileWireTopology implements TopologyInterface {

    public void wire(Graph graph) {
        Set<Integer> nodeCount = new HashSet<>();
        for (Pair<Integer, Integer> connection : ScenarioReader.getInstance().getConnections()) {

            int firstNode = connection.getKey();
            int otherNode = connection.getValue();

            nodeCount.add(firstNode);
            nodeCount.add(otherNode);

            graph.setEdge(firstNode, otherNode);
            graph.setEdge(otherNode, firstNode);
        }

        if (nodeCount.size() != Network.size()) {
            throw new AssertException("The number of nodes in the file is different than ones in the simulation.");
        }
    }
}
