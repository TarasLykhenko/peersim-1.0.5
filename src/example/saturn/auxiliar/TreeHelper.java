package example.saturn.auxiliar;

import example.saturn.StateTreeProtocolInstance;
import peersim.core.Node;

public class TreeHelper {

    public static void printTree(StateTreeProtocolInstance root) {

        StringBuilder buffer = new StringBuilder(50);
        treePrint(root, buffer, "", "");
        System.out.print( buffer.toString() );
    }

    private static void treePrint(StateTreeProtocolInstance node, StringBuilder buffer, String prefix, String childrenPrefix) {
        buffer.append(prefix);
        buffer.append(node.getNodeId());
        buffer.append('\n');

        int numberOfChildren = node.getChildren().values().size();
        int counter = 1;
        for (StateTreeProtocolInstance value : node.getChildren().values()) {

            if (counter != numberOfChildren) {
                treePrint(value, buffer, childrenPrefix + "├── ", childrenPrefix + "│   ");
            } else {
                treePrint(value,buffer, childrenPrefix + "└── ", childrenPrefix + "    ");
            }
            counter++;
        }
    }

    public static void printLatencies(int[][] latencies){

        System.out.print(" |");

        for (int i = 0; i < latencies.length; i++) {
            System.out.print(i + " " );
        }
        System.out.print("\n");

        for (int i = 0; i < latencies.length; i++) {
            System.out.print( "--" );
        }


        for (int i = 0; i < latencies.length; i++) {
            System.out.print( "\n" + i + "|");

            for (int j = 0; j < latencies.length; j++) {
                System.out.print(latencies[i][j] + " ");
            }
        }

    }

    public static void generateLatencies(StateTreeProtocolInstance first, int[][] latencies) {

        DFSSearch((int) first.getNodeId(), (int) first.getNodeId(), first, latencies, 0);

    }

    public static void DFSSearch(int firstNodeId, int originNodeId, StateTreeProtocolInstance node, int[][] latencies, int latencySum){

        int currentNodeID = (int) node.getNodeId();

        for (StateTreeProtocolInstance value : node.getChildren().values()) {
                int newNodeId = (int) value.getNodeId();

                if(originNodeId == newNodeId) continue; //avoid cycles

                int newLatency = getLatency(currentNodeID, newNodeId) + latencySum;
                latencies[firstNodeId][newNodeId] = newLatency;
                DFSSearch(firstNodeId, currentNodeID, value, latencies, newLatency);
        }

        //parent
        if(node.getParent() == null) return; //root case

        int parentID = (int) node.getParent().getNodeId();

        if(originNodeId == parentID) return; //avoid cycles

        int newLatency = getLatency(currentNodeID, parentID) + latencySum;
        latencies[firstNodeId][parentID] = newLatency;
        DFSSearch(firstNodeId, currentNodeID, node.getParent(), latencies, newLatency);
    }

    private static int getLatency(long nodeId, long nodeId1) {
        return  1;
    }
}