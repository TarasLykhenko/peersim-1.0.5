package example.myproject.topology;

import example.myproject.datatypes.AssertException;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.graph.Graph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileWireTopology implements TopologyInterface {

    private static final String PAR_WIRE_FILE_NAME = "topology-file-name";

    public void wire(Graph graph) {
        String fileName = Configuration.getString(PAR_WIRE_FILE_NAME);
        List<String> fileLines;
        try {
            fileLines = Files.readAllLines(Paths.get("example/other/" + fileName));
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Could not find file example/other/" + fileName);
        }

        Set<Integer> nodeCount = new HashSet<>();
        for (String line : fileLines) {
            String[] lineTokens = line.split("-");
            int firstNode = Integer.valueOf(lineTokens[0]);
            int otherNode = Integer.valueOf(lineTokens[1]);
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
