package example.myproject.datatypes;

import java.util.HashMap;
import java.util.Map;

public class Message {

    private Integer group;
    private Long sender;
    private Long forwarder;
    private Map<Long, Integer> data;
    private Map<Long, Integer> metadata;

    public Message(Integer group, Long sender, Long forwarder, Map<Long, Integer> data) {
        this.group = group;
        this.data = new HashMap<>(data);
        this.sender = sender;
        this.forwarder = forwarder;
    }

    public Message(Message message) {
        this.data = message.data;
    }

    public void addMetadata(Map<Long, Integer> metadata) {
        if (this.metadata != null) {
            throw new RuntimeException("Message already contains metadata.");
        }

        this.metadata = metadata;
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

    public Map<Long, Integer> getMetadata() {
        return this.metadata;
    }

    public int getMetadataSize() {
        return this.metadata.size();
    }

}
