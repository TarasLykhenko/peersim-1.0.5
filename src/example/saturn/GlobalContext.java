package example.saturn;

import example.saturn.datatypes.message.types.MetadataMessage;
import peersim.core.CommonState;
import peersim.core.Network;

import java.lang.reflect.Array;
import java.util.*;

public  class GlobalContext {
    public static HashMap<Integer, List<Long>> keysToDcs = new HashMap<>();

    public static HashMap<Integer, Queue<String>> nodeLogger = new HashMap<>(); //records everything
    public static HashMap<Integer, Queue<String>> clientLogger = new HashMap<>(); //records everything

    public static HashMap<Integer, Queue<String>> cycleLogger = new HashMap<>(); //records everything by cycle

    public static HashMap<Long, HashMap<Integer, Integer>> brokerQueue = new HashMap<>(); //records queue size HashMap<cycle, HashMap<nodeID, size>>
    public static HashMap<Long, HashMap<Integer, Integer>> dataQueue = new HashMap<>(); //records queue size


    public static void newNodeLogEntry(int nodeID, String message){

        addToQueue(nodeLogger, nodeID,"c: " + CommonState.getTime() +" nodeID: " + nodeID + " " + message );

        //by cycle
        addToQueue(cycleLogger, (int) CommonState.getTime(),"c: " + CommonState.getTime() +" nodeID: " + nodeID + " " + message );

    }

    public static void newClientLogEntry(int nodeID, String message){

        addToQueue(clientLogger, nodeID,"c: " + CommonState.getTime() +" clientID: " + nodeID + " " + message );

        //by cycle
        addToQueue(cycleLogger, (int) CommonState.getTime(),"c: " + CommonState.getTime() +" clientID: " + nodeID + " " + message );

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


    public static void addValueToQueueStatistics(long cycle, int nodeID, int brokerQueueSize, int dataQueueSize){

        HashMap<Integer, Integer> brokerMap = brokerQueue.get(cycle);
        HashMap<Integer, Integer> dataMap = dataQueue.get(cycle);

        if(brokerMap == null){
            brokerMap = new HashMap<Integer, Integer>();
            dataMap = new HashMap<Integer, Integer>();
            brokerQueue.put(cycle, brokerMap);
            dataQueue.put(cycle, dataMap);
        }

        brokerMap.put(nodeID,brokerQueueSize);
        dataMap.put(nodeID,dataQueueSize);

    }


    public static void printQueueSize(){

        int nodeSize = Network.size();

        for (long i = 0L; i < CommonState.getTime(); i++) {
            HashMap<Integer, Integer> brokerMap = brokerQueue.get(i);
            HashMap<Integer, Integer> dataMap = dataQueue.get(i);

            int brokerMedian = 0;
            int dataMedian = 0;

            for (int j = 0; j < nodeSize; j++) {
                brokerMedian += brokerMap.get(j);
                dataMedian += dataMap.get(j);
            }

            System.out.println("cycle: " + i + " median broker queue: "+ brokerMedian+ " median data queue: "+ dataMedian);

        }
    }

}
