package example.myproject.topology;

import example.myproject.datatypes.AssertException;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.graph.Graph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileWireTopology extends BaseTopology {

    private static final String PAR_WIRE_FILE_NAME = "topology-file-name";
    /**
     *
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    protected FileWireTopology(String prefix) {
        super(prefix);
    }

    /**
     * NOTE: This is looking for files inside example/other
     */
    @Override
    public void wire(Graph graph) {
        String fileName = Configuration.getString(PAR_WIRE_FILE_NAME);
        List<String> fileLines;
        try {
            fileLines = Files.readAllLines(Paths.get("example/other/" + fileName));
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Could not find file example/other/" + fileName);
        }


        Map<String, Integer> stringToNodeId =  new HashMap<>();
        for (String line : fileLines) {
            String[] lineTokens = line.split("-");
            String firstNode = lineTokens[0];
            String otherNode = lineTokens[1];

            if (!stringToNodeId.containsKey(firstNode)) {
                int nodeId = stringToNodeId.size();
                stringToNodeId.put(firstNode, nodeId);
            }

            if (!stringToNodeId.containsKey(otherNode)) {
                int nodeId = stringToNodeId.size();
                stringToNodeId.put(otherNode, nodeId);
            }

            int firstNodeId = stringToNodeId.get(firstNode);
            int otherNodeId = stringToNodeId.get(otherNode);

            graph.setEdge(firstNodeId, otherNodeId);
            graph.setEdge(otherNodeId, firstNodeId);
        }

        if (stringToNodeId.size() != Network.size()) {
            throw new AssertException("The number of nodes in the file is different than ones in the simulation.");
        }

        super.wire(graph);
    }
}
