package example.saturn;

import example.saturn.datatypes.message.types.MetadataMessage;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public  class GlobalContext {
    public static HashMap<Integer, List<Long>> keysToDcs = new HashMap<>();

    public static HashMap<Integer, Queue<String>> logger = new HashMap<>(); //records everything

    public void newLogEntry(int nodeID, String message){
        Queue<String> nodeQueue = logger.get(nodeID);
        if(nodeQueue == null) {
            nodeQueue = new LinkedList<String>();
            logger.put(nodeID,nodeQueue);
        }

        nodeQueue.add(message);
    }



}
