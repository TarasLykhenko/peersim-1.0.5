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

package example.cops;

import com.sun.istack.internal.Nullable;
import example.cops.datatypes.DataObject;
import example.cops.datatypes.EventUID;
import example.cops.datatypes.Operation;
import example.cops.datatypes.VersionVector;
import javafx.util.Pair;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Protocol;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

abstract class StateTreeProtocolInstance
        implements StateTreeProtocol, Protocol {

    private final String prefix;

    //--------------------------------------------------------------------------
    //Fields
    //--------------------------------------------------------------------------

    /**
     * Value held by this protocol
     */
    protected VersionVector metadata = new VersionVector();

    private int reads = 0;
    private int remoteReads = 0;
    private int updates = 0;


    protected VersionVector data = new VersionVector();
    protected int counter = 0;
    protected int epoch = 0;
    protected int largerEpochSeen = 0;
    protected double averageProcessing;
    protected List<EventUID> processed = new ArrayList<>();
    private static final String PAR_LINK_PROT = "linkable";
    private final int link;
    protected Set<String> deliveredRemoteReads = new HashSet<>();

    protected int fullMetadata = 0;
    protected int partialMetadata = 0;

    protected int clientsCycle = 1;
    protected int countProcessed = 0;
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


    //--------------------------------------------------------------------------
    //Initialization
    //--------------------------------------------------------------------------

    /**
     * Does nothing.
     */
    public StateTreeProtocolInstance(String prefix) {
        link = Configuration.getPid(prefix + "." + PAR_LINK_PROT);
        this.prefix = prefix;
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
    public int copsPut(Integer key, long time) {
        // If it's a local write, the write can happen immediatly
        int oldVersion = keyToDOVersion.get(key);
        int newVersion = oldVersion + 1;
        writer.println("write key:"+key+"|ver:"+newVersion + " (local put)");
        doWrite(key, newVersion);
        return newVersion;
    }

    public void copsPutRemote(Integer key, Map<Integer, Integer> context, Integer version) {
        int currentVersion = keyToDOVersion.get(key);
        writer.println("remote receive key:"+key+"|ver:"+version+"| deps:"+context+" | mycontext:"+keyToDOVersion);

        if (currentVersion >= version) {
            // Current version is more updated, ignore the update
            writer.println("Am more updated");
            System.out.println("RETURNING");
            Map<Integer, Integer> newUpdate = new HashMap<>();
            newUpdate.put(key, version);
            checkIfCanAcceptMigratedClients(newUpdate);
            return;
        }
        System.out.println("NOT RETURNING");

        Map<Integer, Integer> missingDeps = checkDeps(context);

        if (missingDeps.isEmpty()) {
            // Depending on a previous write.
            if (version != currentVersion + 1) {
                Map<Integer, Integer> dependencyContext = new HashMap<>();
                dependencyContext.put(key, version - 1);
                System.out.println("ISTO PROVAVELMENTE ESTÁ SUPER MAL PÁ! key:" + key + " | ownVer:" + currentVersion + "|otherVer:"+version);
                System.out.println("writeContext: " + context);
                System.out.println("myContext: " + keyToDOVersion);
                RemoteUpdateQueueEntry entry = new RemoteUpdateQueueEntry(key, version);
                writer.println("putting key:"+key+"|ver:"+version + " on queue");
                updateToContextDeps.put(entry, dependencyContext);
                return;
            }

            writer.println("write key:"+key+"|ver:"+version + " (remote put)");
            doWrite(key, version);
            remoteWrites.add(new Pair<>(key, version));
            System.out.println("unlimited lets go works");

            Map<Integer, Integer> newlyAppliedUpdates =
                    checkIfCanProcessRemoteUpdates(key, version);
            if (!newlyAppliedUpdates.containsKey(key)) {
                newlyAppliedUpdates.put(key, version);
            }
            writer.println("Newly applied updates: " + newlyAppliedUpdates);
            checkIfCanAcceptMigratedClients(newlyAppliedUpdates);
        } else {
            System.out.println("Missing deps: " + missingDeps);
            RemoteUpdateQueueEntry entry = new RemoteUpdateQueueEntry(key, version);
            updateToContextDeps.put(entry, new HashMap<>(missingDeps));
            writer.println("putting deps on queue: key:"+key+"|ver:"+version+"|deps:"+missingDeps+"mycontext:"+keyToDOVersion);
        }
        System.out.println("done");
    }



    void migrateClientQueue(Client client, Map<Integer, Integer> clientContext) {
        incrementRemoteReads();
        Map<Integer, Integer> missingDeps = checkDeps(clientContext);
        if (missingDeps.isEmpty()) {
            writer.println("Accept immediatly client " + client.getId());
            acceptClient(client);
        } else {
            writer.println("Client " + client.getId() + " going to queue. deps:" + missingDeps + "|currentStatus:"+keyToDOVersion);

            clientToDepsQueue.put(client, missingDeps);
        }
    }

    private Map<Integer, Integer> checkIfCanProcessRemoteUpdates(int key, int version) {
        Map<Integer, Integer> newlyAppliedUpdates = new HashMap<>();

        Map<Integer, Integer> result = checkIfCanProcessRemoteUpdatesHelper(key, version);
        while (!result.isEmpty()) {
            newlyAppliedUpdates.putAll(result);
            result.clear();
            for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
                result = checkIfCanProcessRemoteUpdatesHelper(entry.getKey(), entry.getValue());
            }
        }
        writer.println("List of newly applied updates: " + newlyAppliedUpdates);
        return newlyAppliedUpdates;
    }

    /**
     * Given a new applied update, checks if any queued updates relied on this update and
     * if so, checks if the queued updates can be applied.
     * @param key Key of the newly applied update
     * @param version Version of the newly applied update
     * @return A map of all newly applied updates, including the original one and the
     * possible following new ones
     */
    // Verificar se de facto está bem
    private Map<Integer, Integer> checkIfCanProcessRemoteUpdatesHelper(int key, int version) {
        Map<Integer, Integer> newlyAppliedUpdates = new HashMap<>();
        Iterator<RemoteUpdateQueueEntry> it = updateToContextDeps.keySet().iterator();
        System.out.println("Trying to apply.");
        while (it.hasNext()) {
            RemoteUpdateQueueEntry queuedUpdate = it.next();
            Map<Integer, Integer> queuedUpdateContext = updateToContextDeps.get(queuedUpdate);
            @Nullable Integer contextVersion = queuedUpdateContext.get(key);
            if (contextVersion != null) {
                writer.println("Chceking key:"+key+"|ownVer:"+version+"|queuedVer:"+contextVersion);
                if (version >= contextVersion) {
                    System.out.println("Removing! key:"+key+"|ver:"+contextVersion);
                    queuedUpdateContext.remove(key);
                }
            }

            if (queuedUpdateContext.isEmpty()) {
                Integer currentVersion = keyToDOVersion.get(queuedUpdate.key);
                if (currentVersion <= queuedUpdate.version) {
                    writer.println("QUEUD UPDATE! write key:"+queuedUpdate.key+"|ver:"+queuedUpdate.version);
                    doWrite(queuedUpdate.key, queuedUpdate.version);
                } else {
                    writer.println("Could apply update but too old.");
                    System.out.println("TOO OLD!");
                }
                newlyAppliedUpdates.put(queuedUpdate.key, queuedUpdate.version);
                it.remove();
            }
        }

        return newlyAppliedUpdates;
    }

    private void checkIfCanAcceptMigratedClients(Map<Integer, Integer> newUpdates) {
        Iterator<Client> it = clientToDepsQueue.keySet().iterator();
        while (it.hasNext()) {
            Client client = it.next();
            Map<Integer, Integer> clientContext = clientToDepsQueue.get(client);

            for (Integer newUpdateKey : newUpdates.keySet()) {
                // Check if the client context contains the key
                @Nullable Integer contextVersion = clientContext.get(newUpdateKey);
                if (contextVersion != null) {
                    Integer updateVersion = newUpdates.get(newUpdateKey);
                    if (updateVersion >= contextVersion) {
                        System.out.println("YAY!");
                        writer.println("accept client " + client.getId());
                        acceptClient(client);
                        // provavelmente vai dar concurrent iter exception
                        it.remove();
                    }
                }
            }
        }
    }

    private void acceptClient(Client client) {
        clients.add(client);
        client.isWaitingForResult = false;
        receivedMigrations++;
    }

    private void doWrite(Integer key, Integer version) {
        if (keyToDOVersion.get(key) != version - 1) {
            System.out.println("HMMMMMM: ownVer: " + keyToDOVersion.get(key) + "|otherVer:" + version);
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
            keyToDOVersion.put(dataObject.getKey(), 0);
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

    public void setClientsCycle(int clientsCycle) {
        this.clientsCycle = clientsCycle;
    }

    public int getClientsCycle() {
        return clientsCycle;
    }

    //--------------------------------------------------------------------------
    // Replication groups methods
    //--------------------------------------------------------------------------

    // Tbh isto provavelmente pode ser feito usando os proprios datasets locais do objecto
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
    // Processed methods
    //--------------------------------------------------------------------------

    public void addProcessedEvent(EventUID event) {
        if (event.getOperation().getType() == Operation.Type.REMOTE_READ) {
            if (!isAlreadyDelivered(event)) {
                averageProcessing = (averageProcessing + (CommonState.getTime() - event.getTimestamp()));
                countProcessed++;
                this.processed.add(event);
            }
        } else {
            averageProcessing = (averageProcessing + (CommonState.getTime() - event.getTimestamp()));
            countProcessed++;
            this.processed.add(event);
        }
    }

    public void cleanProcessed() {
        this.processed.clear();
    }

    public List<EventUID> getProcessed() {
        return this.processed;
    }

    public String processedToString() {
        StringBuilder result = new StringBuilder();
        for (EventUID eventUID : processed) {
            result.append(" ").append(eventUID.toString());
        }
        return result.toString();
    }

    public String processedToStringFileFormat() {
        StringBuilder result = new StringBuilder();
        for (EventUID eventUID : processed) {
            result.append(" ").append(eventUID.toStringFileFormat());
        }
        return result.toString();
    }

    public double getAverageProcessingTime() {
        if (countProcessed == 0) {
            return 1;
        }
        return averageProcessing / this.countProcessed;
    }

    double getLatencyProcessingTime() {
        if (countProcessed == 0) {
            return 1;
        }
        return (float) averageLatency / this.countProcessed;
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