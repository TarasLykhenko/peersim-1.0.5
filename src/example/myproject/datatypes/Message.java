package example.myproject.datatypes;

import example.myproject.Initialization;
import example.myproject.Sizeable;
import example.myproject.Statistics;
import example.myproject.Utils;
import peersim.config.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// TODO - Remaining optimizations:
//      - De momento cada nó tem de ver entrada a entrada da metadata
//      - (Mas isto até faz sentido, pois ele tem de ver entrada a entrada se faz sentido)
//      - Acho que afinal nem sequer faz sentido optimizar nada disto. Quando ele está a ver
//      - uma entrada que ele deve cortar, ele analisa e depois corta
public class Message implements Sizeable {
    private static long globalId;

    private final int group;
    private final long sender;
    private final List<List<MetadataEntry>> metadata;
    private final Map<Long, Integer> data;
    private Map<Long, Map<Long, Integer>> past;
    private final long forwarder;
    private final long id;
    private Long nextDestination;

    //TODO Ver tamanho do passado e data size maybe
    @Override
    public long getSize() {
        return 8 + // Group
                8 + // Sender
                getMetadataSize() + // Metadata
                (data.size() * 12) + // data
                getPastSize() + // past
                8 + // forwarder
                8 + // id
                8; // nextDestination
    }

    public Message(int group, Map<Long, Integer> data, long sender, long forwarder) {
        this.group = group;
        this.data = data;
        this.sender = sender;
        this.forwarder = forwarder;
        this.metadata = new ArrayList<>();
        this.id = globalId++;

        assertState();
    }

    /**
     * Given an existing message that may contain metadata and a given node,
     * create a new message from it containing the relevant metadata for such path
     * @param message
     */
    public Message(Message message, List<List<MetadataEntry>> metadata, long forwarder, long destination) {
        this.group = message.group;
        this.data = new HashMap<>(message.data);
        this.past = Utils.matrixCopy(message.past);
        this.metadata = metadata;
        this.sender = message.sender;
        this.forwarder = forwarder;
        this.nextDestination = destination;
        this.id = message.id;

        //TODO isto vai ter de ser 2 delta + 1 devido a duplicates 99% certeza
        for (List<MetadataEntry> vector : metadata) {
            while (vector.size() > Utils.DELTA_MAX_SIZE) {
                vector.remove(0);
            }
        }

        updateMaximumSize();
        assertState();
    }

    /**
     * Full copy of another message
     *
     * @param message another message
     */
    public Message(Message message) {
        this.group = message.group;
        this.data = new HashMap<>(message.data);
        this.past = Utils.matrixCopy(message.past);
        this.sender = message.sender;
        this.metadata = Utils.matrixCopy(message.getMetadata());
        this.forwarder = message.forwarder;
        this.id = message.id;
        this.nextDestination = message.nextDestination;

        for (List<MetadataEntry> vector : metadata) {
            while (vector.size() > Utils.DELTA_MAX_SIZE) {
                vector.remove(0);
            }
        }

        assertState();
    }

    public void addPublisherState(Map<Long, Map<Long, Integer>> past) {
        this.past = Utils.matrixCopy(past);
    }

    public int getGroup() {
        return this.group;
    }

    public Map<Long, Integer> getData() {
        return data;
    }

    public Map<Long, Map<Long, Integer>> getPast() {
        return past;
    }

    public long getSender() {
        return this.sender;
    }

    public long getForwarder() {
        return this.forwarder;
    }

    public List<List<MetadataEntry>> getMetadata() {
        return this.metadata;
    }

    public long getNextDestination() {
        return this.nextDestination;
    }

    public long getId() {
        return this.id;
    }

    public long getMetadataSize() {
        return metadata.stream().mapToLong(Collection::size).sum();
    }

    public long getPastSize() {
        long result = 0;
        result += past.size() * 8;
        for (long publisher : past.keySet()) {
            Map<Long, Integer> msgsSentByPublisher = past.get(publisher);
            result += ( msgsSentByPublisher.size() * 12);
        }

        return result;
    }

    public void printMessage() {
        long vectorSize = metadata.stream().mapToLong(Collection::size).max().orElse(0);
        System.out.println("Message " + sender + ": " + data.get(sender) + " | from " + forwarder + " to " + nextDestination);
        System.out.println("Id: " + id + " | Groups: " + group);
        if (Utils.DEBUG_V) {
            System.out.println("Targets: " + data);
            System.out.println("Total size: " + getMetadataSize());
            System.out.println("Biggest vector size: " + vectorSize);
        }
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

    public void printPast() {
        System.out.println("-- Message past -- (sender: " + sender + ")");
        for (long publisher : past.keySet()) {
            Map<Long, Integer> msgsSent = past.get(publisher);
            System.out.println(publisher + " >> " + msgsSent);
        }
    }

    public static MetadataEntry getLastNonNullEntry(List<MetadataEntry> vector) {
        //>> System.out.println("test!");
        //>> System.out.println(vector);
        for (int i = 1; i <= vector.size(); i++) {
            MetadataEntry entry = vector.get(vector.size() - i);
            //>> System.out.println(entry);
            if (entry.getState() != MetadataEntry.State.JUMP) {
                //>> System.out.println("Returning entry " + entry);
                return entry;
            }
        }
        System.out.println("Returning null");
        throw new AssertException("There should always be more than 1 non null entry");
    }

    private void updateMaximumSize() {
        long totalSize = metadata.stream().mapToLong(Collection::size).sum();
        long msgBiggestVectorSize = metadata.stream().mapToLong(Collection::size).max().orElse(0);

        if (Statistics.biggestVectorSize < msgBiggestVectorSize) {
            Statistics.biggestVectorSize = msgBiggestVectorSize;
        }

        if (Statistics.biggestTotalSize < totalSize) {
            Statistics.biggestTotalSize = totalSize;
        }
    }

    public boolean fullyEqual(Message message) {
        if (this == message) {
            return true;
        }

        return  group == message.group &&
                sender == message.sender &&
                forwarder == message.forwarder &&
                id == message.id &&
                nextDestination.equals(message.nextDestination) &&
                Utils.matrixIsEqual(metadata, message.metadata) &&
                data.equals(message.data) &&
                Utils.matrixIsEqual(past, message.past);
    }

    private void assertState() {
        for (List<MetadataEntry> vector : metadata) {
            if (vector.size() > Utils.DELTA_MAX_SIZE) {
                throw new AssertException("The vector has too many MD entries. " + vector);
            }

            for (MetadataEntry entry : vector) {
                if (entry.getState() == MetadataEntry.State.JUMP) {
                    continue;
                }

                NodePath path = Initialization.pathsToPathLongs.get(entry.getPathId());
                if (path.path.size() > Utils.DELTA + 2) {
                    path.printLn("");
                    throw new AssertException("Path is too big. ");
                }
            }
        }
    }
}
