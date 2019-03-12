package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import peersim.config.Configuration;
import peersim.core.CommonState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MessagePublisher {

    /**
     * Each node belongs to a list of groups
     */
    private Set<Integer> groups = new HashSet<>();

    /**
     * Each node may not belong to a group but must forward said group message
     */
    private Set<Integer> forwardingGroups = new HashSet<>();

    /**
     * Subscription routing tables of the neighbours
     */
    private Map<Long, Set<Integer>> neighbourSRTs = new HashMap<>();

    /**
     * This is a reverse neighboursSRTS - Useful for fast message forwarding.
     */
    private Map<Integer, Set<Long>> groupsToNeighbours = new HashMap<>();

    /**
     * Every time a node publishes a new message, it increments the number
     * of sent messages
     */
    private Integer messagesSent = 0;

    private final long id;
    private final int restTime;
    private final int restTimeInterval;

    private long nextAction = 0;

    MessagePublisher(long id) {
        this.id = id;
        restTime = Configuration.getInt("rest_time");
        restTimeInterval = Configuration.getInt("rest_time_interval");
    }

    Message publishMessage() {
        if (shouldPublishMessage()) {
            return generateNewMessage();
        } else {
            return null;
        }
    }

    private boolean shouldPublishMessage() {
        if (CommonState.getTime() > nextAction) {
            int interval = CommonState.r.nextInt(restTimeInterval * 2) - restTimeInterval;
            nextAction = CommonState.getTime() + restTime + interval;
            return true;
        } else {
            return false;
        }
    }

    private Message generateNewMessage() {
        List<Integer> listGroups = new ArrayList<>(groups);
        int listIdx = CommonState.r.nextInt(listGroups.size());
        int groupEntry = listGroups.get(listIdx);
        messagesSent++;
        Map<Long, Integer> data = new HashMap<>();
        data.put(id, messagesSent);
        return new Message(groupEntry, data, id, id);
    }


    // ------------------
    // INTERFACE METHODS
    // ------------------

    Set<Integer> getCopyOfSRT() {
        return new HashSet<>(groups);
    }

    /** Returns the nodes that are interested in a message.
     */
    Set<Long> getInterestedNodes(Message message) {
        int group = message.getGroup();

        return groupsToNeighbours.get(group);
    }

    void addNeighbourSRT(Long neighbourId, Set<Integer> srt) {
        this.neighbourSRTs.put(neighbourId, srt);

        for (Integer group : srt) {
            groupsToNeighbours.computeIfAbsent(group, k -> new HashSet<>())
                    .add(neighbourId);
        }
    }

    void addGroup(Integer group) {
        this.groups.add(group);
    }

    boolean belongsToGroup(Integer group) {
        return this.groups.contains(group);
    }

    void setForwarderOfGroup(Integer group) {
        if (this.groups.contains(group)) {
            throw new AssertException("Forwarding a group that the node belongs to?");
        }

        this.forwardingGroups.add(group);
    }

    String printStatus() {
        return "Messages sent: " + messagesSent;
    }
}
