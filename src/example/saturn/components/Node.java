package example.saturn.components;

import com.sun.jdi.Value;
import example.common.BasicClientInterface;
import example.common.datatypes.DataObject;
import example.saturn.Client;
import example.saturn.StateTreeProtocol;
import peersim.core.Protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Node implements StateTreeProtocol, Protocol {

    private int reads = 0;
    private int remoteReads = 0;
    private int updates = 0;

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

    @Override
    public int timestamp() {
        return 0;
    }

    @Override
    public boolean isInterested(int key) {
        return false;
    }

    @Override
    public void addClients(Set<Client> clientList) {

    }

    @Override
    public int get(Integer key) {
        return 0;
    }

    @Override
    public int put(Integer key, Value value) {
        return 0;
    }

    @Override
    public void putRemote(Integer key, Integer version) {

    }

    @Override
    public Set<? extends BasicClientInterface> getClients() {
        return null;
    }

    //--------------------------------------------------------------------------
    // Statistics methods
    //--------------------------------------------------------------------------

    @Override
    public int getNumberUpdates() {
        return updates;
    }

    @Override
    public void incrementUpdates() {
        updates++;
    }

    @Override
    public void incrementRemoteReads() {
        remoteReads++;
    }

    @Override
    public void incrementLocalReads() {
        reads++;
    }

    @Override
    public int getNumberRemoteReads() {
        return remoteReads;
    }

    @Override
    public int getNumberLocalReads() {
        return reads;
    }

    @Override
    public void addNewReadCompleted(long timeToComplete) {

    }

    @Override
    public void addNewUpdateCompleted(long timeToComplete) {

    }

    @Override
    public long getAverageReadLatency() {
        return 0;
    }

    @Override
    public long getAverageUpdateLatency() {
        return 0;
    }

    @Override
    public void setNodeId(Long nodeId) {

    }

    @Override
    public long getNodeId() {
        return nodeID;
    }

    @Override
    public int getQueuedClients() {
        return 0;
    }

    @Override
    public Object clone() {
        return null;
    }
}
