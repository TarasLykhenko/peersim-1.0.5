package example.saturn;

import example.saturn.datatypes.message.types.Message;
import example.saturn.datatypes.message.types.MetadataMessage;
import example.saturn.datatypes.message.types.RemoteUpdateMessage;

import java.util.*;

public class Broker {

    //tree logic
    StateTreeProtocolInstance parent;
    HashMap<Long, StateTreeProtocolInstance> children = new HashMap<>();

    Queue<MetadataMessage> metaQueue = new LinkedList<>();

    public void newUpdate(long newUpdateId) {

        List<Long> remoteBrokers = getRemoteBrokersID();
        for (long remoteReplicaID : remoteBrokers) {

            MetadataMessage message = new MetadataMessage(newUpdateId);
            message.setNodeDestinationID(remoteReplicaID);
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
