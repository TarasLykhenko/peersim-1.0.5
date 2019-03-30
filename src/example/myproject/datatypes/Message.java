package example.myproject.datatypes;

import example.myproject.Sizeable;
import example.myproject.Statistics;
import example.myproject.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final List<Long> travelledPath;
    private final long forwarder;
    private final long id;
    private final Long nextDestination;

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
        this.travelledPath = new ArrayList<>();
        this.nextDestination = null;
        this.travelledPath.add(sender);
    }

    /**
     * Given an existing message that may contain metadata and a given node,
     * create a new message from it containing the relevant metadata for such path
     *
     * @param message
     */
    public Message(Message message, List<List<MetadataEntry>> metadata, long forwarder, long destination) {
        this.group = message.group;
        this.data = new HashMap<>(message.data);
        this.past = Utils.matrixCopy(message.past);
        this.metadata = metadata;
        this.sender = message.sender;
        this.forwarder = forwarder;
        this.travelledPath = new ArrayList<>(message.travelledPath);
        this.nextDestination = destination;
        this.id = message.id;

        for (List<MetadataEntry> vector : metadata) {
            while (vector.size() > Utils.DELTA_MAX_SIZE) {
                vector.remove(0);
            }
        }

        updateMaximumSize();
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
        this.travelledPath = new ArrayList<>(message.travelledPath);

        for (List<MetadataEntry> vector : metadata) {
            while (vector.size() > Utils.DELTA_MAX_SIZE) {
                vector.remove(0);
            }
        }
    }

    //TODO? Acho que não é preciso fazer isto tbh

    /**
     * Merges two message's metadata, in order to remove jumps
     *
     * @param message
     * @return
     */
    public Message merge(Message message) {
        if (id != message.id) {
            throw new AssertException("Attemping to merge different messages");
        }
        if (!nextDestination.equals(message.nextDestination)) {
           // throw new AssertException("Messages have different destinations?!");
        }
        if (forwarder != message.forwarder) {
         //   throw new AssertException("Messages have different forwarders?!");
        }
        if (metadata.size() != message.metadata.size()) {
            // TODO falta fazer isto
            throw new AssertException("Merging messages with different vectors");
        }

        if (Utils.DEBUG_V) {
            System.out.println("MERGING");
            printMessage();
            message.printMessage();
        }

        List<List<MetadataEntry>> newMetadata = new ArrayList<>();

        for (int i = 0; i < metadata.size(); i++) {
            List<MetadataEntry> newVector = new ArrayList<>();
            newMetadata.add(newVector);

            List<MetadataEntry> vectorOne = metadata.get(i);
            List<MetadataEntry> vectorTwo = message.metadata.get(i);

            if (vectorOne.size() != vectorTwo.size()) {
                System.out.println("S1: " + vectorOne.size() + " | S2: " + vectorTwo.size());
                throw new AssertException("Vectors must have the same size. " + System.lineSeparator() +
                        "(Vector 1: " + vectorOne + " | Vector 2: " + vectorTwo + ")");
            }

            for (int j = 0; j < vectorTwo.size(); j++) {
                MetadataEntry entryOne = vectorOne.get(j);
                MetadataEntry entryTwo = vectorTwo.get(j);

                if (entryOne.getState() == MetadataEntry.State.JUMP) {
                    newVector.add(entryTwo);
                } else {
                    newVector.add(entryOne);
                }

                if (    entryOne.getState() != MetadataEntry.State.JUMP &&
                        entryTwo.getState() != MetadataEntry.State.JUMP) {
                    if (!entryOne.equals(entryTwo)) {
                        throw new AssertException("Entries that are not jumps should be equal.");
                    }
                }
            }
        }

        Message mergedMessage = new Message(this, newMetadata, message.forwarder, message.nextDestination);

        if (Utils.DEBUG_V) {
            if (mergedMessage.fullyEqual(message)) {
                System.out.println("Merged message is equal to the argument message");
            } else if (mergedMessage.fullyEqual(this)) {
                System.out.println("Merged message is equal to the original message");
            } else {
                System.out.println("Merged message is UNIQUE!");
            }
            mergedMessage.printMessage();
            System.out.println("MERGE COMPLETE");
        }


        return mergedMessage;
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
            result += (msgsSentByPublisher.size() * 12);
        }

        return result;
    }

    public void printMessage() {
        long vectorSize = metadata.stream().mapToLong(Collection::size).max().orElse(0);
        System.out.println("Message " + sender + ": " + data.get(sender) + " | from " + forwarder + " to " + nextDestination);
        System.out.println("Id: " + id + " | Groups: " + group);
        System.out.println("Travelled path: " + travelledPath);
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

        return group == message.group &&
                sender == message.sender &&
                forwarder == message.forwarder &&
                id == message.id &&
                nextDestination.equals(message.nextDestination) &&
                Utils.matrixIsEqual(metadata, message.metadata) &&
                data.equals(message.data) &&
                Utils.matrixIsEqual(past, message.past);
    }

    public void addNewNodeToTravelPath(long nodeId) {
        travelledPath.add(nodeId);
    }

    /**
     * Returns true if there are more than delta jumps.
     *
     * @return
     */
    public boolean hasTooManyJumps() {
        int numberJumps = 0;

        //System.out.println("CHECKING MESSAGE!");
        //printMessage();

        // Functional programming
        long sum = metadata.stream().map(
                vector -> vector.stream()
                        .filter(entry -> entry.getState() == MetadataEntry.State.JUMP)
                        .count())
                .mapToLong(Long::intValue).sum();

        // Vanilla programming
        numberJumps = getNumberJumps();

        if (sum != numberJumps) {
            throw new AssertException("Functional programming failed :^(");
        }

        return numberJumps > Utils.DELTA;
    }

    private int getNumberJumps() {
        int numberJumps = 0;

        for (List<MetadataEntry> vector : metadata) {
            for (MetadataEntry entry : vector) {
                if (entry.getState() == MetadataEntry.State.JUMP) {
                    numberJumps++;
                }
            }
        }

        return numberJumps;
    }
}
