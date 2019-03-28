package example.myproject.topology;


import peersim.config.Configuration;
import peersim.core.Network;
import peersim.graph.Graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TreeWireTopology implements TopologyInterface {

    private static final String PAR_BREADTH = "tree-breadth";

    private final int breadth;

    TreeWireTopology() {
        this.breadth = Configuration.getInt(PAR_BREADTH);
    }

    private int cursor = 1;
    @Override
    public void wire(Graph graph) {
        List<Integer> nextLine = new ArrayList<>();
        nextLine.add(0);

        while (cursor < Network.size()) {
            BaseTopology.debug("NextLine: " + nextLine);
            nextLine = handleNextLevel(graph, nextLine);
        }
    }

    private List<Integer> handleNextLevel(Graph graph, List<Integer> currentLine) {
        List<Integer> nextLine = new ArrayList<>();

        while (!currentLine.isEmpty()) {
            int counter = 0;
            int currentBranch = currentLine.get(0);
            currentLine.remove(0);
            while (counter < breadth) {
                if (cursor == Network.size()) {
                    return Collections.emptyList();
                }
                counter++;
                graph.setEdge(currentBranch, cursor);
                graph.setEdge(cursor, currentBranch);
                nextLine.add(cursor++);
            }
        }
        return nextLine;
    }
}
