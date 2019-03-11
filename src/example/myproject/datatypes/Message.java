package example.myproject.datatypes;

import example.myproject.server.PathHandler;
import peersim.config.Configuration;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// TODO - Remaining optimizations:
//      - De momento cada nó tem de ver entrada a entrada da metadata
//      - (Mas isto até faz sentido, pois ele tem de ver entrada a entrada se faz sentido)
//      - Acho que afinal nem sequer faz sentido optimizar nada disto. Quando ele está a ver
//      - uma entrada que ele deve cortar, ele analisa e depois corta
public class Message {

    private final Integer group;
    private final Long sender;
    private final List<Map<Long, Integer>> metadata;
    private Map<Long, Integer> data;
    private Long forwarder;

    private Long nextDestination;

    public Message(Integer group, Long sender, Long forwarder) {
        this.group = group;
        // this.data = new HashMap<>(data);
        this.sender = sender;
        this.forwarder = forwarder;
        this.metadata = new ArrayList<>();
    }

    /**
     * Given an existing message that may contain metadata and a given node,
     * create a new message from it containing the relevant metadata for such path
     * @param message
     */
    public Message(Message message, List<Map<Long, Integer>> metadata, long destination) {
        this.group = message.group;
        this.data = message.data;
        this.metadata = metadata;
        this.sender = message.sender;
        this.nextDestination = destination;


        /*
        for (Map<Long, Integer> metadataVector : message.metadata) {
            Map<Long, Integer> result = new MessageMap<>();
            for (Long pathId : metadataVector.keySet()) {
                if (pathHandler.pathIsSubpath(pathId, path)) {
                    result.put(pathId, metadataVector.get(pathId));
                }
            }
            if (!result.isEmpty()) {
                metadata.add(result);
            }
        }
        setNextDestination(path.get(0).getID());
        */
    }

    public void addPublisherState(Map<Long, Integer> data) {
        this.data = data;
    }

    /**
     * Having a list of different
     * @param pathId
     * @param counter
     */
    public void addMetadata(int idx, Long pathId, Integer counter) {
        if (this.metadata != null) {
            throw new RuntimeException("Message already contains metadata.");
        }

        if (metadata.get(idx) == null) {
            metadata.add(new HashMap<>());
        }

        this.metadata.get(idx).put(pathId, counter);
    }

    public int getGroup() {
        return this.group;
    }

    public Map<Long, Integer> getData() {
        return data;
    }

    public Long getSender() {
        return this.sender;
    }

    public Long getForwarder() {
        return this.forwarder;
    }

    public List<Map<Long, Integer>> getMetadata() {
        return this.metadata;
    }

    public Long getNextDestination() {
        return this.nextDestination;
    }

    public int getMetadataSize() {
        return this.metadata.size();
    }

}
