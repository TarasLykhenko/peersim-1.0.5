package example.saturn;

import example.saturn.datatypes.message.types.MetadataMessage;
import peersim.core.CommonState;

import java.lang.reflect.Array;
import java.util.*;

public  class GlobalContext {
    public static HashMap<Integer, List<Long>> keysToDcs = new HashMap<>();

    public static HashMap<Integer, Queue<String>> nodeLogger = new HashMap<>(); //records everything
    public static HashMap<Integer, Queue<String>> clientLogger = new HashMap<>(); //records everything

    public static HashMap<Integer, Queue<String>> cycleLogger = new HashMap<>(); //records everything by cycle


    public static void newNodeLogEntry(int nodeID, String message){

        addToQueue(nodeLogger, nodeID,"c: " + CommonState.getTime() +" nodeID: " + nodeID + " " + message );

        //by cycle
        addToQueue(cycleLogger, (int) CommonState.getTime(),"c: " + CommonState.getTime() +" nodeID: " + nodeID + " " + message );

    }

    public static void newClientLogEntry(int nodeID, String message){

        addToQueue(clientLogger, nodeID,"c: " + CommonState.getTime() +" clientID: " + nodeID + " " + message );

        //by cycle
        addToQueue(cycleLogger, (int) CommonState.getTime(),"c: " + CommonState.getTime() +" clientID: " + nodeID + " " + message );
        System.out.println("c: " + CommonState.getTime() +" clientID: " + nodeID + " " + message );

    }

    public static void  addToQueue(HashMap<Integer, Queue<String>> map, int mapEntry, String m){

        Queue<String> nodeQueue = map.get(mapEntry);
        if(nodeQueue == null) {
            nodeQueue = new LinkedList<String>();
            map.put(mapEntry, nodeQueue);
        }

        nodeQueue.add(m);
    }

    public static void printLog(){

        /*for (Map.Entry<Integer, Queue<String>> entry : nodeLogger.entrySet()) {

            for (String item: entry.getValue()) {
                System.out.println(item);
            }
        }

        for (Map.Entry<Integer, Queue<String>> entry : clientLogger.entrySet()) {

            for (String item: entry.getValue()) {
                System.out.println(item);
            }
        }
*/
        for (int i = 0; i < (int) CommonState.getTime(); i++){
            Queue<String> queue = cycleLogger.get(i);

            if(queue == null) continue;

            for (String item: queue) {
                System.out.println(item);
            }
        }


    }


}
