package example.saturn.components;

import java.util.HashMap;

public class Node {

    Long nodeID;
    Node parent;
    HashMap<Long, Node> children = new HashMap<>();

    public Node(Long _nodeID, Node _parent){
        nodeID = _nodeID;
        parent = _parent;

    }

    public void addChild(Node child){
        children.put(child.nodeID, child);
    }

}
