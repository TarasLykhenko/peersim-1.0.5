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
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.core.Protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
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

    protected int localReads = 0;
    protected int remoteReads = 0;
    protected int updates = 0;


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

    protected Map<UUID, Client> pendingClientsQueue = new HashMap<>();


    protected Map<Integer, Map<Integer, Integer>> updatesQueue = new HashMap<>();
    protected long sentMigrations = 0;
    protected long receivedMigrations = 0;

    private Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
    private Map<Integer, Set<DataObject>> levelToDataObjects = new HashMap<>();
    private Set<DataObject> allDataObjects = new HashSet<>();

    private Map<Integer, DataObject> keyToDataObject = new HashMap<>();

    /**
     * We map
     */
    private Map<Integer, Integer> keyToDOVersion = new HashMap<>();

    /**
     * Super ineficient lookup, everytime there's a new update will check every deps.
     * Will improve performance with fast lookup table if needed later-
     */


    private Map<RemoteUpdateQueueEntry, Map<Integer, Integer>> updateToContextDeps = new HashMap<>();

    /**
     * Queue of clients that migrated and have some remaining dependencies
     */
    private Map<Client, Map<Integer, Integer>> clientToDepsQueue = new HashMap<>();


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
        return keyToDOVersion.get(key);
    }

    /**
     * Returns the new version of the object after writing
     */
    public int copsPut(Integer key) {
        // If it's a local write, the write can happen immediatly
        int oldVersion = keyToDOVersion.get(key);
        int newVersion = oldVersion + 1;
        doWrite(key, newVersion);
        return newVersion;
    }

    public void copsPutRemote(Integer key, Map<Integer, Integer> context, Integer version) {
        int currentVersion = keyToDOVersion.get(key);
        if (currentVersion >= version) {
            // Current version is more updated, ignore the update
            return;
        }

        Map<Integer, Integer> missingDeps = checkDeps(context);

        if (missingDeps.isEmpty()) {
            if (version != currentVersion + 1) {
                System.out.println("ISTO PROVAVELMENTE ESTÁ SUPER MAL PÁ!");
                return;
            }

            doWrite(key, version);

            Map<Integer, Integer> newlyAppliedUpdated =
                    checkIfCanProcessRemoteUpdates(key, version);
            checkIfCanAcceptMigratedClients(newlyAppliedUpdated);
        } else {
            RemoteUpdateQueueEntry entry = new RemoteUpdateQueueEntry(key, version);
            updateToContextDeps.put(entry, new HashMap<>(context));
        }
    }

    void migrateClientQueue(Client client, Map<Integer, Integer> clientContext) {
        Map<Integer, Integer> missingDeps = checkDeps(clientContext);
        if (missingDeps.isEmpty()) {
            acceptClient(client);
        } else {
            clientToDepsQueue.put(client, clientContext);
        }
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
    private Map<Integer, Integer> checkIfCanProcessRemoteUpdates(int key, int version) {
        Map<Integer, Integer> newAppliedUpdates = new HashMap<>();

        for (RemoteUpdateQueueEntry queuedUpdate : updateToContextDeps.keySet()) {
            Map<Integer, Integer> updateContext = updateToContextDeps.get(queuedUpdate);
            @Nullable Integer contextVersion = updateContext.get(key);
            if (contextVersion != null) {
                if (version >= contextVersion) {
                    updateContext.remove(key);
                }
            }

            if (updateContext.isEmpty()) {
                doWrite(queuedUpdate.key, queuedUpdate.version);
                newAppliedUpdates.put(queuedUpdate.key, queuedUpdate.version);
            }
        }

        return newAppliedUpdates;
    }

    private void checkIfCanAcceptMigratedClients(Map<Integer, Integer> newUpdates) {
        for (Client client : clientToDepsQueue.keySet()) {
            Map<Integer, Integer> clientContext = clientToDepsQueue.get(client);

            for (Integer newUpdateKey : newUpdates.keySet()) {
                // Check if the client context contains the key
                @Nullable Integer contextVersion = clientContext.get(newUpdateKey);
                if (contextVersion != null) {
                    Integer updateVersion = newUpdates.get(newUpdateKey);
                    if (updateVersion >= contextVersion) {
                        acceptClient(client);
                        // provavelmente vai dar concurrent iter exception
                        clientToDepsQueue.remove(client);
                    }
                }
            }
        }
    }

    private void acceptClient(Client client) {
        clients.add(client);
        receivedMigrations++;
    }

    private void doWrite(Integer key, Integer version) {
        keyToDOVersion.put(key, version);
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

    public void addFullMetadata(int update) {
        fullMetadata += update;
    }

    public void addPartialMetadata(int update) {
        partialMetadata += update;
    }

    public double getFullMetadata() {
        return ((double) fullMetadata) / ((double) updates);
    }

    public double getPartialMetadata() {
        return ((double) partialMetadata) / ((double) updates);
    }

    public void incrementUpdates() {
        updates++;
    }

    public void incrementRemoteReads() {
        remoteReads++;
    }

    public void incrementLocalReads() {
        localReads++;
    }

    public int getNumberUpdates() {
        return updates;
    }

    public int getNumberRemoteReads() {
        return remoteReads;
    }

    public int getNumberLocalReads() {
        return localReads;
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

        private int key;
        private int version;

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