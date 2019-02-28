package example.myproject;

import peersim.core.Node;

import java.util.List;
import java.util.Set;

public interface BackendInterface {

    /**
     * Sets the id of the protocol to be the same as the node's Id the protocol belongs to
     * @param id The node Id
     */
    void setId(Long id);

    /**
     * Returns the nodeId
     * @return nodeId
     */
    Long getId();

    /**
     * Makes the server belong to a new group,
     * allowing it to publish messages of said group
     * @param group An integer, similar to a topic-based PubSub
     */
    void addGroup(Integer group);

    /**
     * Returns true if the server belongs to a given group
     * @param group The topic-based group
     * @return true if the server belongs to the group, otherwise false
     */
    boolean belongsToGroup(Integer group);

    /**
     * Makes the server start forwarding messages of a given group
     * (The server would "publish" messages of this group
     * @param group The topic-based group
     */
    void setForwarderOfGroup(Integer group);

    /**
     * Returns a fresh copy of the server's SRT, containing the groups
     * it belongs to and those that it forwards
     */
    Set<Integer> getCopyOfSRT();

    /**
     * Adds a neighbour's SRT to the current state of the server
     *
     * @param neighbourId The nodeId of the neighbour
     * @param srt The neighbour's SRT
     */
    void addNeighbourSRT(Long neighbourId, Set<Integer> srt);

    /**
     * Returns a collection of lists, each list being a path to a given node
     * (There are no intermediary paths in these lists, meaning each path always ends
     * at the most distant possible node it can reach)
     */
    Set<List<Node>> getNeighbourhood();

    /**
     * Sets the server's neighbourhood
     * @param differentPaths A set of lists of all
     *                       different possible paths up to a given distance
     */
    void setNeighbourHood(Set<List<Node>> differentPaths);
}
