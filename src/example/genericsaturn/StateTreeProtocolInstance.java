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

package example.genericsaturn;

import example.common.BasicClientInterface;
import example.common.datatypes.DataObject;
import example.genericsaturn.datatypes.EventUID;
import example.genericsaturn.datatypes.PendingEventUID;
import example.genericsaturn.datatypes.VersionVector;
import peersim.config.Configuration;
import peersim.core.Linkable;
import peersim.core.Node;
import peersim.core.Protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class StateTreeProtocolInstance
        implements StateTreeProtocol, Protocol {

    //--------------------------------------------------------------------------
    //Fields
    //--------------------------------------------------------------------------

    /**
     * Value held by this protocol
     */
    protected VersionVector metadata = new VersionVector();
    private final int tree = 2; //This is very hardcoded cba.

    protected int localReads = 0;
    protected int remoteReads = 0;
    protected int updates = 0;


    protected Queue<EventUID> metadataQueue = new LinkedList<>();
    protected VersionVector data = new VersionVector();
    protected int counter = 0;
    protected int epoch = 0;
    protected int largerEpochSeen = 0;
    protected double averageProcessing;
    protected Map<Long, List<EventUID>> queue = new HashMap<>();
    protected Map<Integer, List<PendingEventUID>> pendingQueue = new HashMap<>();
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
    protected Map<UUID, Client> pendingClientsQueue = new HashMap<>();

    protected long nodeId;

    protected long sentMigrations = 0;
    protected long receivedMigrations = 0;

    private Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
    private Map<Integer, Set<DataObject>> levelToDataObjects = new HashMap<>();
    private Map<Integer, DataObject> keyToDataObject = new HashMap<>();
    private Set<DataObject> allDataObjects = new HashSet<>();


    //--------------------------------------------------------------------------
    //Initialization
    //--------------------------------------------------------------------------

    /**
     * Does nothing.
     */
    StateTreeProtocolInstance(String prefix) {
        link = Configuration.getPid(prefix + "." + PAR_LINK_PROT);
    }

    //--------------------------------------------------------------------------

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
    public Set<? extends BasicClientInterface> getClients() {
        Set<Client> result = new HashSet<>();
        result.addAll(clients);
        result.addAll(pendingClientsQueue.values());
        return result;
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

    @Override
    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

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

    public boolean isInterested(int key) {
        // System.out.println(keyToDataObject.keySet().stream().map(Object::toString).collect(Collectors.joining("-")));
        // System.out.println("Contains key " + key + ": " + keyToDataObject.containsKey(key));
        return keyToDataObject.containsKey(key);
    }

    //--------------------------------------------------------------------------
    // Queue methods
    //--------------------------------------------------------------------------

    @Override
    public void initQueue(Node node) {

        Linkable linkn = (Linkable) node.getProtocol(link);
        for (int i = 0; i < linkn.degree(); i++) {
            Node neighbor = linkn.getNeighbor(i);
            queue.put(neighbor.getID(), new ArrayList<>());
        }
        this.nodeId = node.getID();
    }

    public void addToQueue(EventUID event, Long from) {
        // System.out.println("Trying to add event");
        for (Long key : queue.keySet()) {
            if (!key.equals(from)) {
                //    System.out.println("Adding to queue!");
                queue.get(key).add(event);
            }
        }
    }

    @Override
    public List<EventUID> getQueue(Long node) {
        return queue.get(node);
    }

    public void cleanQueue(Long node) {
        if (queue.containsKey(node)) {
            queue.get(node).clear();
        }
    }

    public void processQueue(List<EventUID> queue, long id) {
        for (EventUID event : queue) {
            if (isInterested(event.getOperation().getKey()) ||
                    (event.isMigration() && id == event.getMigrationTarget())) {
                // System.out.println("Adding metadata!");
                addMetadata(event);
            }
        }
    }

    private Set<UUID> migrationLabelQueue = new HashSet<>();

    public void migrateClientQueue(Client client, EventUID label) {
        pendingClientsQueue.put(label.getIdentifier(), client);
        if (migrationLabelQueue.contains(label.getIdentifier())) {
            migrationLabelQueue.remove(label.getIdentifier());
            acceptClient(label);
            client.instantMigrationAccept();
        }
    }

    public void acceptClient(EventUID event) {
        Client client = pendingClientsQueue.get(event.getIdentifier());

        clients.add(client);
        idToClient.put(client.getId(), client);
        receivedMigrations++;
        client.migrationOver(nodeId);
        pendingClientsQueue.remove(event.getIdentifier());
    }

    public void addQueueToQueue(List<EventUID> queue, Long from) {
        for (EventUID eventUID : queue) {
            this.addToQueue(eventUID, from);
        }
    }

    //--------------------------------------------------------------------------
    // Pending queue methods
    //--------------------------------------------------------------------------

    public void addToPendingQueue(EventUID event, int epoch, long senderId) {
        //    System.out.println("Adding event: "+event.toString()+" to node "+senderId+" pending queue");
        if (pendingQueue.containsKey(epoch)) {
            pendingQueue.get(epoch).add(new PendingEventUID(event, senderId));
        } else {
            List<PendingEventUID> v = new ArrayList<>();
            pendingQueue.put(epoch, v);
            v.add(new PendingEventUID(event, senderId));
        }
    }

    public List<PendingEventUID> getPendingQueue(int epoch) {
        return pendingQueue.get(epoch);
    }

    public void cleanPendingQueue(int currentEpoch) {
        Set<Integer> epochs = pendingQueue.keySet();
        Integer[] list = new Integer[epochs.size()];
        int c = 0;
        for (int element : epochs) {
            list[c] = element;
            c++;
        }
        for (int epoch : list) {
            if (epoch < currentEpoch) {
                pendingQueue.remove(epoch);
            }
        }
    }


    public void addQueueToPendingQueue(List<EventUID> queue2, int epoch, long senderId) {
        for (EventUID eventUID : queue2) {
            this.addToPendingQueue(eventUID, epoch, senderId);
        }
    }

    @Override
    public void processPendingEpoch(int currentEpoch) {
        for (int epoch : pendingQueue.keySet()) {
            if (epoch < currentEpoch) {
                List<PendingEventUID> v = getPendingQueue(epoch);
                if (v != null) {
                    //System.out.println("cleaning pendings of epoch "+epoch+" - "+v.toString());
                    for (int i = 0; i < v.size(); i++) {
                        PendingEventUID event = v.get(i);
                        this.addToQueue(event.getEvent(), event.getSenderId());
                    }
                }
            }
        }
    }

    public Collection<Client> getQueuedClientsClients() {
        return pendingClientsQueue.values();
    }

    @Override
    public int getQueuedClients() {
        return pendingClientsQueue.size();
    }

    //--------------------------------------------------------------------------
    // Processed methods
    //--------------------------------------------------------------------------

    // TODO CBA arranjar isto para versão sem remote read.
    /*
    public void addProcessedEvent(EventUID event) {
        //System.out.println("PROCESSING EVENT!");
        //System.out.println("Processing event "+event.getKey()+","+event.getTimestamp()+" from: "+event.getSrc()+" to: "+event.getDst()+" with latency: "+event.getLatency()+
        //		" tool "+(getEpoch() - event.getEpoch())+" cycles");
        if (event.getOperation().getType() == Operation.Type.REMOTE_READ) {
            if (!isAlreadyDelivered(event)) {
                averageProcessing = (averageProcessing + (getEpoch() - event.getEpoch()));
                countProcessed++;
                this.processed.add(event);
            }
        } else {
            averageProcessing = (averageProcessing + (getEpoch() - event.getEpoch()));
            countProcessed++;
            this.processed.add(event);
        }
    }
    */

    public void addProcessedEvent(EventUID event) {
        //System.out.println("PROCESSING EVENT!");
        //System.out.println("Processing event "+event.getKey()+","+event.getTimestamp()+" from: "+event.getSrc()+" to: "+event.getDst()+" with latency: "+event.getLatency()+
        //		" tool "+(getEpoch() - event.getEpoch())+" cycles");
        if (event.isMigration() && nodeId == event.getMigrationTarget()) {
            if (pendingClientsQueue.containsKey(event.getIdentifier())) {
                acceptClient(event);
            } else {
                migrationLabelQueue.add(event.getIdentifier());
            }
        }

        if (!isAlreadyDelivered(event)) {
            averageProcessing = (averageProcessing + (getEpoch() - event.getEpoch()));
            countProcessed++;
            this.processed.add(event);
        } else {
            averageProcessing = (averageProcessing + (getEpoch() - event.getEpoch()));
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
        String result = "";
        for (int i = 0; i < processed.size(); i++) {
            result = result + " " + processed.get(i).toString();
        }
        return result;
    }

    public String processedToStringFileFormat() {
        String result = "";
        for (int i = 0; i < processed.size(); i++) {
            result = result + " " + processed.get(i).toStringFileFormat();
        }
        return result;
    }

    public double getAverageProcessingTime() {
        if (countProcessed == 0) {
            return 1;
        }
        return averageProcessing / this.countProcessed;
    }

    public double getLatencyProcessingTime() {
        if (countProcessed == 0) {
            return 1;
        }
        return averageLatency / this.countProcessed;
    }

    //--------------------------------------------------------------------------
    // Data/metadata methods
    //--------------------------------------------------------------------------

    @Override
    public void addMetadata(EventUID event) {
        if (metadataQueue.isEmpty()) {
            if (event.isMigration()) {
                addProcessedEvent(event);
            } else if (data.seenEvent(event.getOperation().getKey(), event.getTimestamp())) {
                addProcessedEvent(event);
            } else {
                metadataQueue.add(event);
            }
        } else {
            metadataQueue.add(event);
        }
    }

    @Override
    public VersionVector getMetadataVector() {
        return metadata;
    }

    @Override
    public void addData(EventUID event, Object datum) {
        // averageLatency = averageLatency + event.getLatency();
        data.addEvent(event.getOperation().getKey(), event.getTimestamp());
        boolean matches = true;
        while (!metadataQueue.isEmpty() && (matches)) {
            EventUID head = metadataQueue.peek();
            if (head.isMigration()) {
                metadataQueue.poll();
                addProcessedEvent(head);
            }
            if (data.seenEvent(head.getOperation().getKey(), head.getTimestamp())) {
                metadataQueue.poll();
                addProcessedEvent(event);
            } else {
                matches = false;
            }
        }
    }

    @Override
    public VersionVector getDataVector() {
        return data;
    }

    //--------------------------------------------------------------------------
    // Epoch methods
    //--------------------------------------------------------------------------

    public int newEpoch() {
        epoch++;
        return epoch;
    }

    public int getEpoch() {
        return epoch;
    }

    public int largerEpochSeen() {
        return largerEpochSeen;
    }

    public void updateLargerEpochSeen(int newEpoch) {
        if (newEpoch > largerEpochSeen) {
            largerEpochSeen = newEpoch;
        }
    }

    //--------------------------------------------------------------------------
    // Timestamp methods
    //--------------------------------------------------------------------------

    public int timestamp() {
        counter++;
        return counter;
    }

    public Map<Integer, Set<StateTreeProtocol>> getLevelsToNodes() {
        return levelsToNodes;
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

    @Override
    public Object clone() {
        return null;
    }
}