package example.oldsaturn.datastructures;

import java.util.Map;
import java.util.TreeSet;

public class Dictionary {

    private Map<Integer, TreeSet<Integer>> graph;
    private Map<Integer, TreeSet<Integer>> replicas;
    private Map<Integer, TreeSet<Integer>> masters;

    public Map<Integer, TreeSet<Integer>> getGraph() {
        return graph;
    }

    public void setGraph(Map<Integer, TreeSet<Integer>> graph) {
        this.graph = graph;
    }

    public Map<Integer, TreeSet<Integer>> getReplicas() {
        return replicas;
    }

    public void setReplicas(Map<Integer, TreeSet<Integer>> replicas) {
        this.replicas = replicas;
    }

    public Map<Integer, TreeSet<Integer>> getMasters() {
        return masters;
    }

    public void setMasters(Map<Integer, TreeSet<Integer>> masters) {
        this.masters = masters;
    }

}
