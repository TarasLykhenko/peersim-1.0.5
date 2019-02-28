package example.myproject.topology;


import peersim.config.Configuration;
import peersim.core.Network;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// TODO
public class TreeWireTopology extends WireGraph {

    private static final String PAR_BREADTH = "breadth";
    private static final String PAR_DEBUG = "debug";

    private final int breadth;
    private final boolean DEBUG;

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    public TreeWireTopology(String prefix) {
        super(prefix);
        this.breadth = Configuration.getInt(prefix + "." + PAR_BREADTH);
        this.DEBUG = Configuration.getBoolean(PAR_DEBUG);
    }

    private int cursor = 1;
    @Override
    public void wire(Graph graph) {
        List<Integer> nextLine = new ArrayList<>();
        nextLine.add(0);

        while (cursor < Network.size()) {
            debug("NextLine: " + nextLine);
            nextLine = handleNextLevel(graph, nextLine);
        }

        for (int i = 0; i < graph.size(); i++) {
            debug(i + "'s neighbours: " + graph.getNeighbours(i));
        }
    }

    private List<Integer> handleNextLevel(Graph graph, List<Integer> currentLine) {
        List<Integer> nextLine = new ArrayList<>();

        while (!currentLine.isEmpty()) {
            int counter = 0;
            int currentBranch = currentLine.get(0);
            while (counter < breadth) {
                if (cursor == Network.size()) {
                    return Collections.emptyList();
                }
                counter++;
                graph.setEdge(currentBranch, cursor);
                graph.setEdge(cursor, currentBranch);
                nextLine.add(cursor++);
            }
            currentLine.remove(0);
        }
        return nextLine;
    }

    private void debug(String string) {
        if (DEBUG) {
            System.out.println(string);
        }
    }
}
