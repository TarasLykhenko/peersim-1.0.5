package example.myproject.datatypes;

import example.myproject.Initialization;
import peersim.config.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO - Remaining optimizations:
//      - De momento cada nó tem de ver entrada a entrada da metadata
//      - (Mas isto até faz sentido, pois ele tem de ver entrada a entrada se faz sentido)
//      - Acho que afinal nem sequer faz sentido optimizar nada disto. Quando ele está a ver
//      - uma entrada que ele deve cortar, ele analisa e depois corta
public class Message {

    private static final int delta;

    static {
        String PAR_DELTA = "delta";
        delta = Configuration.getInt(PAR_DELTA);
    }

    private final Integer group;
    private final Long sender;
    private final List<List<MetadataEntry>> metadata;
    private final Map<Long, Integer> data;
    private final Long forwarder;

    private Long nextDestination;

    public Message(Integer group, Map<Long, Integer> data, Long sender, Long forwarder) {
        this.group = group;
        this.data = data;
        this.sender = sender;
        this.forwarder = forwarder;
        this.metadata = new ArrayList<>();
    }

    /**
     * Given an existing message that may contain metadata and a given node,
     * create a new message from it containing the relevant metadata for such path
     * @param message
     */
    public Message(Message message, List<List<MetadataEntry>> metadata, long forwarder, long destination) {
        this.group = message.group;
        this.data = message.data;
        this.metadata = metadata;
        this.sender = message.sender;
        this.forwarder = forwarder;
        this.nextDestination = destination;

        for (List<MetadataEntry> vector : metadata) {
            if (vector.size() > delta * 2) {
                vector.remove(0);
            }
        }
    }

    public void addPublisherState(Map<Long, Integer> data) {
        this.data.putAll(data);
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
            metadata.add(new ArrayList<>());
        }

        this.metadata.get(idx).add(new MetadataEntry(pathId, counter));
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

    public List<List<MetadataEntry>> getMetadata() {
        return this.metadata;
    }

    public Long getNextDestination() {
        return this.nextDestination;
    }

    public int getMetadataSize() {
        return this.metadata.size();
    }

    public void printMessage() {
        System.out.println("Message " + sender + ": " + data.get(sender) + " | from: " + forwarder + " to: " + nextDestination);
        System.out.println("Group: " + group);
        System.out.println("Vectors: ");
        for (List<MetadataEntry> vector : metadata) {
            StringBuilder stringBuilder = new StringBuilder();
            for (MetadataEntry entry : vector) {
                NodePath nodePath = Initialization.pathsToPathLongs.get(entry.getPathId());
                nodePath.getPathString();
                stringBuilder.append(nodePath.getPathString() + " - " + entry.getValue() + ", ");
            }
            System.out.println("  " + stringBuilder.toString());
        }
        System.out.println();
    }
}
