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

package example.saturn;

import example.common.datatypes.DataObject;
import example.saturn.components.Node;
import javafx.util.Pair;
import peersim.core.Protocol;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
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

    protected Set<String> deliveredRemoteReads = new HashSet<>();

    protected long averageLatency = 0;

    protected Set<Client> clients = new HashSet<>();
    protected Map<Integer, Client> idToClient = new HashMap<>();

    protected long nodeId;

    protected long sentMigrations = 0;
    protected long receivedMigrations = 0;

    private Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
    private Map<Integer, Set<DataObject>> levelToDataObjects = new HashMap<>();
    private Set<DataObject> allDataObjects = new HashSet<>();

    private Map<Integer, DataObject> keyToDataObject = new HashMap<>();

    /**
     * We map
     */
    Map<Integer, Integer> keyToDOVersion = new HashMap<>();

    /**
     * Super ineficient lookup, everytime there's a new update will check every deps.
     * Will improve performance with fast lookup table if needed later-
     */


    Map<RemoteUpdateQueueEntry, Map<Integer, Integer>> updateToContextDeps = new HashMap<>();

    /**
     * Queue of clients that migrated and have some remaining dependencies
     */
    Map<Client, Map<Integer, Integer>> clientToDepsQueue = new HashMap<>();

    List<Pair<Integer, Integer>> remoteWrites = new ArrayList<>();
    PrintWriter writer;

    StateTreeProtocol parent;
    HashMap<Long, StateTreeProtocol> children = new HashMap<>();

    public void addChild(StateTreeProtocol child){
        children.put(child.getNodeId(), child);
    }

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

    public int copsGet(Integer key) {
        incrementLocalReads();
        return keyToDOVersion.get(key);

    }

    /**
     * Returns the new version of the object after writing
     */
    public int copsPut(Integer key) {
        // If it's a local write, the write can happen immediatly
        int oldVersion = keyToDOVersion.get(key);
        int newVersion = oldVersion + 1;
        writer.println("write key:"+key+"|ver:"+newVersion + " (local put)");
        doWrite(key, newVersion);

        checkIfCanProcessRemoteUpdates();
        checkIfCanAcceptMigratedClients();

        return newVersion;
    }

    public void copsPutRemote(Integer key, Map<Integer, Integer> context, Integer version) {
        int currentVersion = keyToDOVersion.get(key);
        writer.println("remote receive key:"+key+"|ver:"+version+"| deps:"+context+" | mycontext:"+keyToDOVersion);

        if (currentVersion >= version) {
            // Current version is more updated, ignore the update
            writer.println("Am more updated");
            debug("RETURNING");
            Map<Integer, Integer> newUpdate = new HashMap<>();
            newUpdate.put(key, version - 1);
            updateToContextDeps.put(new RemoteUpdateQueueEntry(key, version), newUpdate);
            checkIfCanProcessRemoteUpdates();
            checkIfCanAcceptMigratedClients();
            return;
        }
        debug("NOT RETURNING");

        Map<Integer, Integer> missingDeps = checkDeps(context);

        if (missingDeps.isEmpty()) {
            // Depending on a previous write.
            if (version != currentVersion + 1) {
                Map<Integer, Integer> dependencyContext = new HashMap<>();
                dependencyContext.put(key, version - 1);
                debug("ISTO PROVAVELMENTE ESTA SUPER MAL PA! key:" + key + " | ownVer:" + currentVersion + "|otherVer:"+version);
                debug("writeContext: " + context);
                debug("myContext: " + keyToDOVersion);
                RemoteUpdateQueueEntry entry = new RemoteUpdateQueueEntry(key, version);
                writer.println("putting key:"+key+"|ver:"+version + " on queue");
                updateToContextDeps.put(entry, dependencyContext);
                return;
            }

            writer.println("write key:"+key+"|ver:"+version + " (remote put)");
            doWrite(key, version);
            remoteWrites.add(new Pair<>(key, version));
            debug("unlimited lets go works");

            checkIfCanProcessRemoteUpdates();
            checkIfCanAcceptMigratedClients();
        } else {
            debug("Missing deps: " + missingDeps);
            RemoteUpdateQueueEntry entry = new RemoteUpdateQueueEntry(key, version);
            updateToContextDeps.put(entry, new HashMap<>(missingDeps));
            writer.println("putting deps on queue: key:"+key+"|ver:"+version+"|deps:"+missingDeps+"mycontext:"+keyToDOVersion);
        }
        debug("done");
    }



    void migrateClientQueue(Client client, Map<Integer, Integer> clientContext) {
        incrementRemoteReads();
        Map<Integer, Integer> missingDeps = checkDeps(clientContext);
        if (missingDeps.isEmpty()) {
            debug("Accept immediatly client " + client.getId());
            acceptClient(client);
            client.instantMigrationAccept();
        } else {
            debug("Client " + client.getId() + " going to queue. deps:" + missingDeps + "|currentStatus:"+keyToDOVersion);

            clientToDepsQueue.put(client, missingDeps);
        }
    }

    /**
     * Given a new applied update, checks if any queued updates relied on this update and
     * if so, checks if the queued updates can be applied.
     */
    // Verificar se de facto está bem
    private void checkIfCanProcessRemoteUpdates() {
        boolean appliedUpdate = true;

        while (appliedUpdate) {
            appliedUpdate = false;
            Set<RemoteUpdateQueueEntry> toRemove = new HashSet<>();

            for (RemoteUpdateQueueEntry queuedUpdate : updateToContextDeps.keySet()) {

                Map<Integer, Integer> missingDeps = checkDeps(updateToContextDeps.get(queuedUpdate));
                if (missingDeps.isEmpty()) {
                    Integer currentVersion = keyToDOVersion.get(queuedUpdate.key);
                    if (currentVersion <= queuedUpdate.version) {
                        writer.println("QUEUD UPDATE! write key:"+queuedUpdate.key+"|ver:"+queuedUpdate.version);
                        doWrite(queuedUpdate.key, queuedUpdate.version);
                    } else {
                        writer.println("Could apply update but too old. ownVer:" + currentVersion + "|otherVer:"+queuedUpdate.version);
                        debug("TOO OLD!");
                    }
                    appliedUpdate = true;
                    toRemove.add(queuedUpdate);
                    writer.println("Going to remove key:"+queuedUpdate.key+"|ver:"+queuedUpdate.version);
                } else {
                    addDependency(queuedUpdate, missingDeps);
                }
            }

            for (RemoteUpdateQueueEntry entry : toRemove) {
                updateToContextDeps.remove(entry);
            }
        }
    }

    private void addDependency(RemoteUpdateQueueEntry entry, Map<Integer, Integer> context) {
        updateToContextDeps.put(entry, context);
    }

    void checkIfCanAcceptMigratedClients() {
        Iterator<Client> it = clientToDepsQueue.keySet().iterator();
        while (it.hasNext()) {
            Client client = it.next();
            Map<Integer, Integer> clientContext = clientToDepsQueue.get(client);
            Map<Integer, Integer> missingDeps = checkDeps(clientContext);
            if (missingDeps.isEmpty()) {
                writer.println("accept client " + client.getId());
                acceptClient(client);
                it.remove();
            }
        }
    }

    private void acceptClient(Client client) {
        clients.add(client);
        idToClient.put(client.getId(), client);
        client.migrationOver(nodeId);
        receivedMigrations++;
    }

    private void doWrite(Integer key, Integer version) {
        if (keyToDOVersion.get(key) != version - 1) {
            debug("HMMMMMM: ownVer: " + keyToDOVersion.get(key) + "|otherVer:" + version);
        }

        keyToDOVersion.put(key, version);
        incrementUpdates();
    }

    private Map<Integer, Integer> checkDeps(Map<Integer, Integer> context) {
        Map<Integer, Integer> missingDeps = new HashMap<>();
        for (Integer contextKey : context.keySet()) {
            if (!keyToDOVersion.containsKey(contextKey)) {
                // Partial replication, this datacenter does not contain
                // the dependency object
                continue;
            }
            int contextVersion = context.get(contextKey);
            int currentVersion = keyToDOVersion.get(contextKey);

            if (contextVersion > currentVersion) {
                missingDeps.put(contextKey, contextVersion);
            }
        }

        return missingDeps;
    }




    //--------------------------------------------------------------------------
    // Delivered remote methods
    //--------------------------------------------------------------------------

    @Override
    public long getNodeId() {
        return nodeId;
    }

    @Override
    public int getQueuedClients() {
        return clientToDepsQueue.size();
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
    public void addClients(Set<Client> clientList) {
        clients.addAll(clientList);
        for (Client client : clientList) {
            idToClient.put(client.getId(), client);
        }
    }

    /**
     * Used for statistics ONLY.
     * @return Sum of current clients + queued clients
     */
    @Override
    public Set<Client> getClients() {
        Set<Client> result = new HashSet<>();
        result.addAll(clients);
        result.addAll(clientToDepsQueue.keySet());
        return result;
    }

    //--------------------------------------------------------------------------
    // Replication groups methods
    //--------------------------------------------------------------------------

    @Override
    public boolean isInterested(int key) {
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