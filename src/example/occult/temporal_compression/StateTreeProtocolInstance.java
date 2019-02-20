/*
 * Copyright (c) 2003-2005 The BISON Project
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

package example.occult.temporal_compression;

import example.common.datatypes.DataObject;
import example.occult.ClientInterface;
import example.occult.GroupsManager;
import example.occult.StateTreeProtocol;
import example.occult.datatypes.EventUID;
import example.occult.datatypes.OccultMasterWrite;
import example.occult.datatypes.OccultReadResult;
import javafx.util.Pair;
import peersim.core.Protocol;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

abstract class StateTreeProtocolInstance
        implements StateTreeProtocol, Protocol {

    //--------------------------------------------------------------------------
    //Fields
    //--------------------------------------------------------------------------

    /**
     * Value held by this protocol
     */
    private int reads = 0;
    private long totalReadLatency = 0;
    private int remoteReads = 0;
    private int updates = 0;

    protected int counter = 0;

    protected double averageProcessing;
    protected List<EventUID> processed = new ArrayList<>();

    protected Set<String> deliveredRemoteReads = new HashSet<>();

    protected long averageLatency = 0;

    protected Set<ClientInterface> clients = new HashSet<>();
    protected Map<Integer, ClientInterface> idToClient = new HashMap<>();

    protected long nodeId;

    protected long sentMigrations = 0;
    protected long receivedMigrations = 0;

    private Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
    private Map<Integer, Set<DataObject>> levelToDataObjects = new HashMap<>();
    private Set<DataObject> allDataObjects = new HashSet<>();

    private Map<Integer, DataObject> keyToDataObject = new HashMap<>();

    /**
     * This stores the "writes".
     * Maps the key with the causal timestamp.
     */
    Map<Integer, Map<Integer, Integer>> keysToCausalTimestamps = new HashMap<>();
    Map<Integer, Integer> keysToCatchAll = new HashMap<>();

    /**
     * Maps the shardId to the number of updates the shard received.
     */
    Map<Integer, Integer> shardStamps = new HashMap<>();


    List<Pair<Integer, Integer>> remoteWrites = new ArrayList<>();
    PrintWriter writer;


    //--------------------------------------------------------------------------
    //Initialization
    //--------------------------------------------------------------------------

    /**
     * Does nothing.
     */
    public StateTreeProtocolInstance() {
    }

    //--------------------------------------------------------------------------

    /**
     * Clones the value holder.
     */
    public Object clone() {
        return null;
    }

    //--------------------------------------------------------------------------
    //methods
    //--------------------------------------------------------------------------

    public long getAverageLatency() {
        return averageLatency;
    }

    public void setAverageLatency(long averageLatency) {
        this.averageLatency = averageLatency;
    }

    public double getAverageProcessing() {
        return averageProcessing;
    }

    public void setAverageProcessing(double averageProcessing) {
        this.averageProcessing = averageProcessing;
    }

    // --------------------
    // COPS methods
    // --------------------

    public OccultReadResult occultRead(Integer key) {
        incrementLocalReads();

        int shardId = GroupsManager.getInstance().getShardId(key);

        Map<Integer, Integer> deps = new HashMap<>(keysToCausalTimestamps.get(key));
        int catchAll = keysToCatchAll.get(key);
        int shardStamp = shardStamps.get(shardId);
        boolean isMaster = GroupsManager.getInstance()
                .isShardMaster(shardId, this);

        return new OccultReadResult(shardId, deps,catchAll, shardStamp, isMaster);
    }

    /**
     * Returns the new version of the object after writing
     */
    public OccultMasterWrite occultWriteMaster(int key, Map<Integer, Integer> deps, int catchAll) {
        int shardId = GroupsManager.getInstance().getShardId(key);

        StateTreeProtocol masterServer = GroupsManager.getInstance().getMasterServer(shardId);
        if (!masterServer.equals(this)) {
            throw new RuntimeException("Writing to a slave?! I am "
                    + this.getNodeId() + ", master is " + masterServer.getNodeId() + ", shard is " + shardId);
        }


        int newTimeShard = shardStamps.get(shardId) + 1;

        Map<Integer, Integer> updatedDeps = new HashMap<>(deps);
        if (!updatedDeps.containsKey(shardId)) {
//            throw new RuntimeException("wow");
        }
        updatedDeps.put(shardId, newTimeShard);

        shardStamps.put(shardId, newTimeShard);
        keysToCausalTimestamps.put(key, new HashMap<>(updatedDeps));
        keysToCatchAll.put(key, catchAll);

        return new OccultMasterWrite(updatedDeps, newTimeShard);
    }

    public void occultWriteSlave(int key, Map<Integer, Integer> deps, int catchAll, int shardStamp) {
        int shardId = GroupsManager.getInstance().getShardId(key);

        shardStamps.put(shardId, shardStamp);
        keysToCausalTimestamps.put(key, new HashMap<>(deps));
        keysToCatchAll.put(key, catchAll);
    }

    /**
     * Migrations in occult can be instant
     */
    void migrateClientQueue(ClientInterface client) {
        incrementRemoteReads();
        acceptClient(client);
    }

    private void acceptClient(ClientInterface client) {
        clients.add(client);
        idToClient.put(client.getId(), client);
        client.migrationOver();
        receivedMigrations++;
    }


    public Map<Integer, Integer> getShardStamps() {
        return this.shardStamps;
    }


    //--------------------------------------------------------------------------
    // Delivered remote methods
    //--------------------------------------------------------------------------

    public boolean isAlreadyDelivered(EventUID event) {
        return deliveredRemoteReads.contains(event.getOperation().getKey() + "," + event.getTimestamp());
    }

    public void addRemoteRead(EventUID event) {
        deliveredRemoteReads.add(event.getOperation().getKey() + "," + event.getTimestamp());
    }

    @Override
    public long getNodeId() {
        return nodeId;
    }

    @Override
    public void setLevelsToNodes(Map<Integer, Set<StateTreeProtocol>> levelsToNodes) {
        this.levelsToNodes = levelsToNodes;
    }

    @Override
    public Set<StateTreeProtocol> getLevelsToNodes(Integer level) {
        return levelsToNodes.get(level);
    }

    @Override
    public void addDataObjectsToLevel(Set<DataObject> dataObjects, int level) {
        levelToDataObjects.put(level, dataObjects);
        for (DataObject dataObject : dataObjects) {
            keyToDataObject.put(dataObject.getKey(), dataObject);
            keysToCausalTimestamps.put(dataObject.getKey(), new HashMap<>());
            keysToCatchAll.put(dataObject.getKey(), 0);
        }
    }

    @Override
    public Set<DataObject> getDataObjectsFromLevel(int level) {
        return levelToDataObjects.get(level);
    }

    //--------------------------------------------------------------------------
    // Statistics methods
    //--------------------------------------------------------------------------

    @Override
    public int getNumberUpdates() {
        return updates;
    }

    @Override
    public void incrementUpdates() {
        updates++;
    }

    @Override
    public void incrementRemoteReads() {
        remoteReads++;
    }

    @Override
    public void incrementLocalReads() {
        reads++;
    }

    @Override
    public int getNumberRemoteReads() {
        return remoteReads;
    }

    @Override
    public int getNumberLocalReads() {
        return reads;
    }

    @Override
    public void addNewReadCompleted(long timeToComplete) {

    }

    @Override
    public void addNewUpdateCompleted(long timeToComplete) {

    }

    @Override
    public long getAverageReadLatency() {
        return 0;
    }

    @Override
    public long getAverageUpdateLatency() {
        return 0;
    }

    //--------------------------------------------------------------------------
    // Client methods
    //--------------------------------------------------------------------------

    @Override
    public void addClients(Set<ClientInterface> clientList) {
        clients.addAll(clientList);
        for (ClientInterface client : clientList) {
            idToClient.put(client.getId(), client);
        }
    }

    @Override
    public Set<ClientInterface> getClients() {
        return this.clients;
    }

    //--------------------------------------------------------------------------
    // Replication groups methods
    //--------------------------------------------------------------------------

    @Override
    public boolean isInterested(int key) {
        //    System.out.println("Checking key " + key);
        return keyToDataObject.containsKey(key);
    }

    @Override
    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
        String pathfile = "output/" + nodeId + "-output.txt";
        FileWriter fr = null;
        try {
            fr = new FileWriter(pathfile, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter br = new BufferedWriter(fr);
        writer = new PrintWriter(br);
    }

    //--------------------------------------------------------------------------
    // Timestamp methods
    //--------------------------------------------------------------------------

    public int timestamp() {
        counter++;
        return counter;
    }

    public Set<DataObject> getAllDataObjects() {
        if (allDataObjects.isEmpty()) {
            allDataObjects.addAll(levelToDataObjects
                    .values().stream().flatMap(Collection::stream).collect(Collectors.toSet()));
        }
        return allDataObjects;
    }

    public Map<Integer, Set<DataObject>> getAllDataObjectsPerLevel() {
        return levelToDataObjects;
    }

    private void debug(String s) {
        // System.out.println(s);
    }

    class RemoteUpdateQueueEntry {

        int key;
        int version;

        RemoteUpdateQueueEntry(int key, int version) {
            this.key = key;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof  RemoteUpdateQueueEntry)) {
                return false;
            }
            RemoteUpdateQueueEntry entry = (RemoteUpdateQueueEntry) o;
            return (this.key == entry.key) && (this.version == entry.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, version);
        }
    }
}