package example.myproject.datatypes;

import peersim.config.Configuration;

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

    private static final int delta;

    static {
        delta = Configuration.getInt("delta");
    }

    private Integer group;
    private Long sender;
    private Long forwarder;
    private Map<Long, Integer> data;
    private List<Map<Long, Integer>> metadata = new ArrayList<>();

    private Long nextDestination;

    public Message(Integer group, Long sender, Long forwarder) {
        this.group = group;
        // this.data = new HashMap<>(data);
        this.sender = sender;
        this.forwarder = forwarder;
    }

    public Message(Message message) {
        this.data = message.data;
    }

    public void addPublisherState(Map<Long, Integer> data) {
        this.data = data;
    }

    /**
     * Having a list of different
     * @param pathId
     * @param counter
     */
    public void addMetadata(Long pathId, Integer counter) {
        if (this.metadata != null) {
            throw new RuntimeException("Message already contains metadata.");
        }

        this.metadata.put(pathId, counter);
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

    // TODO talvez juntar isto tudo num metodo dentro do Message
    public void setNextDestination(Long nextDestination) {
        this.nextDestination = nextDestination;
    }

    public Long getNextDestination() {
        return this.nextDestination;
    }

    public int getMetadataSize() {
        return this.metadata.size();
    }

    private static class MessageMap<K, V> extends LinkedHashMap<K, V> {
        // TODO verificar se isto está bem
        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
            return this.size() > (delta * 2);
        }
    }
}
