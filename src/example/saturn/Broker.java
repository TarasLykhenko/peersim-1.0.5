package example.saturn;

import example.saturn.datatypes.message.types.Message;
import example.saturn.datatypes.message.types.MetadataMessage;
import example.saturn.datatypes.message.types.RemoteUpdateMessage;

import java.util.*;

public class Broker {

    //tree logic
    StateTreeProtocolInstance parent;
    HashMap<Long, StateTreeProtocolInstance> children = new HashMap<>();

    Storage storage;
    long nodeID;
    Queue<MetadataMessage> metaQueue = new LinkedList<>();

    public Broker(long nodeID){
        this.nodeID = nodeID;
    }

    public void newRemoteMetadataUpdate(MetadataMessage MetadataMessage) {

        long updateID = MetadataMessage.getUpdateID();
        storage.remoteMetadata(updateID);

        //propagate metadata expect to message origin
        List<Long> remoteBrokers = getRemoteBrokersID();
        for (long remoteReplicaID : remoteBrokers) {

            if(remoteReplicaID == MetadataMessage.getNodeOriginID()) continue;;

            MetadataMessage message = new MetadataMessage(updateID);
            message.setNodeDestinationID(remoteReplicaID);
            message.setNodeOriginID(this.nodeID);
            metaQueue.add(message);

        }
    }

    public void newUpdate(long newUpdateId) {

        List<Long> remoteBrokers = getRemoteBrokersID();
        for (long remoteReplicaID : remoteBrokers) {

            MetadataMessage message = new MetadataMessage(newUpdateId);
            message.setNodeDestinationID(remoteReplicaID);
            message.setNodeOriginID(this.nodeID);
            metaQueue.add(message);

        }
    }

    public Message getMessage(){
        return metaQueue.poll();
    }

    public List<Long> getRemoteBrokersID(){
        List<Long> remoteBrokers = new ArrayList<>();

        remoteBrokers.add(parent.getNodeId());

        for (StateTreeProtocolInstance value : getChildren().values()) {
            remoteBrokers.add(value.getNodeId());
        }

        return remoteBrokers;
    }

    public void addChild(StateTreeProtocolInstance child){
        children.put(child.getNodeId(), child);
    }

    public HashMap<Long, StateTreeProtocolInstance> getChildren(){
        return children;
    }

    public StateTreeProtocolInstance getParent(){
        return parent;
    }

    public void setParent(StateTreeProtocolInstance _parent){
        parent = _parent;
    }
}
