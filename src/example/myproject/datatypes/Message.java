package example.myproject.datatypes;

import example.myproject.Initialization;
import example.myproject.Utils;
import peersim.config.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// TODO - Remaining optimizations:
//      - De momento cada nó tem de ver entrada a entrada da metadata
//      - (Mas isto até faz sentido, pois ele tem de ver entrada a entrada se faz sentido)
//      - Acho que afinal nem sequer faz sentido optimizar nada disto. Quando ele está a ver
//      - uma entrada que ele deve cortar, ele analisa e depois corta
public class Message {

    private static final int delta;
    private static long globalId;


    static {
        String PAR_DELTA = "delta";
        delta = Configuration.getInt(PAR_DELTA);
    }

    private final Integer group;
    private final Long sender;
    private final List<List<MetadataEntry>> metadata;
    private final Map<Long, Integer> data;
    private final Long forwarder;
    private final long id;

    private Long nextDestination;

    public Message(Integer group, Map<Long, Integer> data, Long sender, Long forwarder) {
        this.group = group;
        this.data = data;
        this.sender = sender;
        this.forwarder = forwarder;
        this.metadata = new ArrayList<>();
        this.id = globalId++;
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
        this.id = message.id;

        //TODO isto vai ter de ser 2 delta + 1 devido a duplicates 99% certeza
        for (List<MetadataEntry> vector : metadata) {
            while (vector.size() > delta + 1) {
                vector.remove(0);
            }
        }
    }

    /**
     * Full copy of another message
     *
     * @param message another message
     */
    public Message(Message message) {
        this.group = message.group;
        this.data = new HashMap<>(message.data);
        this.sender = message.sender;
        this.metadata = Utils.matrixCopy(message.getMetadata());
        this.forwarder = message.forwarder;
        this.id = message.id;
        this.nextDestination = message.nextDestination;

        for (List<MetadataEntry> vector : metadata) {
            while (vector.size() > delta + 1) {
                vector.remove(0);
            }
        }
    }

    public void addPublisherState(Map<Long, Integer> data) {
        this.data.putAll(data);
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

    public long getId() {
        return this.id;
    }

    public int getMetadataSize() {
        return this.metadata.size();
    }

    public void printMessage() {
        System.out.println("Message " + sender + ": " + data.get(sender) + " | from " + forwarder + " to " + nextDestination);
        System.out.println("Id: " + id);
        System.out.println("Group: " + group);
        System.out.println("Destination: " + nextDestination);
        System.out.println("Targets: " + data);
        System.out.println("Vectors: (" + metadata.size() + ")");
        for (List<MetadataEntry> vector : metadata) {
            StringBuilder stringBuilder = new StringBuilder();
            for (MetadataEntry entry : vector) {
                stringBuilder.append(entry).append(", ");
            }
            System.out.println("  " + stringBuilder.toString());
        }
        System.out.println();
    }
}
