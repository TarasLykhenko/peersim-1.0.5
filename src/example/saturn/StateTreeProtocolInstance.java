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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;

import example.saturn.datatypes.EventUID;
import example.saturn.datatypes.PendingEventUID;
import example.saturn.datatypes.VersionVector;
import peersim.config.Configuration;
import peersim.core.*;

public class StateTreeProtocolInstance
        implements StateTreeProtocol, Protocol {

    //--------------------------------------------------------------------------
    //Fields
    //--------------------------------------------------------------------------

    /**
     * Value held by this protocol
     */
    protected VersionVector metadata;
    protected Queue<EventUID> metadataQueue;
    protected VersionVector data;
    protected int counter;
    protected int epoch;
    protected int largerEpochSeen;
    protected double averageProcessing;
    protected Map<Long, Vector<EventUID>> queue;
    protected Map<Integer, Vector<PendingEventUID>> pendingQueue;
    protected Vector<EventUID> processed;
    private static final String PAR_LINK_PROT = "linkable";
    private final int link;
    protected Map<Integer, List<Integer>> replicationGroups;
    protected int localReads, remoteReads, updates;
    protected Set<String> deliveredRemoteReads;
    protected int fullMetadata, partialMetadata;
    protected int clientsCycle;
    protected int totalServers;
    protected int countProcessed;
    protected long averageLatency;
    protected List<Client> clients;

    protected Map<UUID, Client> pendingClientsQueue = new HashMap<>();
    protected long sentMigrations;
    protected long receivedMigrations;
    //protected TreeSet<Integer> friends;

    //--------------------------------------------------------------------------
    //Initialization
    //--------------------------------------------------------------------------

    /**
     * Does nothing.
     */
    public StateTreeProtocolInstance(String prefix) {
        link = Configuration.getPid(prefix + "." + PAR_LINK_PROT);
        metadata = new VersionVector();
        metadataQueue = new LinkedList<>();
        data = new VersionVector();
        counter = 0;
        epoch = 0;
        largerEpochSeen = 0;
        queue = new HashMap<>();
        processed = new Vector<>();
        pendingQueue = new HashMap<>();
        replicationGroups = new HashMap<>();
        clients = new ArrayList<>();
        localReads = 0;
        remoteReads = 0;
        updates = 0;
        fullMetadata = 0;
        partialMetadata = 0;
        deliveredRemoteReads = new HashSet<>();
        clientsCycle = 1;
        countProcessed = 0;
        averageLatency = 0;
        totalServers = 0;
        //friends = new TreeSet<Integer>();
    }

    //--------------------------------------------------------------------------

    /**
     * Clones the value holder.
     */
    public Object clone() {
        StateTreeProtocolInstance svh = null;
        try {
            svh = (StateTreeProtocolInstance) super.clone();
            svh.cloneQueue(queue);
            svh.cloneMetadataQueue(metadataQueue);
            svh.clonePendingQueue(pendingQueue);
            svh.cloneProcessed(processed);
            svh.cloneMetadata(metadata);
            svh.cloneData(data);
            svh.cloneReplicationGroups(replicationGroups);
            //svh.cloneFriends(friends);
            svh.cloneClients(clients);
            svh.cloneStatistics(localReads, remoteReads, updates, fullMetadata, partialMetadata);
            svh.cloneDeliveredRemoteReads(deliveredRemoteReads);
            svh.setClientsCycle(clientsCycle);
            svh.setCountProcessed(this.countProcessed);
            svh.setAverageLatency(averageLatency);
            svh.setAverageProcessing(averageProcessing);
            svh.setTotalServers(totalServers);
            svh.clonePendingClients(pendingClientsQueue);
        } catch (CloneNotSupportedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //svh = new DoubleVectorHolder("");
        return svh;
    }

    //--------------------------------------------------------------------------
    //methods
    //--------------------------------------------------------------------------

    public void setTotalServers(int totalServers2) {
        totalServers = totalServers2;
        // TODO Auto-generated method stub

    }

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
    // Clone methods
    //--------------------------------------------------------------------------
    public void cloneMetadataQueue(Queue<EventUID> metadataQueueInit) {
        metadataQueue = new LinkedList<>();
        for (EventUID event : metadataQueueInit) {
            metadataQueue.add(new EventUID(event.getKey(), event.getTimestamp(), event.getEpoch(), event.getRemoteRead(), event.getLatency(), event.getSrc(), event.getDst()));
        }
    }

    public void cloneProcessed(Vector<EventUID> processedInit) {
        processed = new Vector<>();
        processed.addAll(processedInit);
    }

    public void cloneQueue(Map<Long, Vector<EventUID>> queueInit) {
        queue = new HashMap<>();
        for (Long key : queueInit.keySet()) {
            queue.put(key, (Vector<EventUID>) queueInit.get(key).clone());
        }
    }

    public void clonePendingQueue(Map<Integer, Vector<PendingEventUID>> pendingQueueInit) {
        pendingQueue = new HashMap<>();
        for (int key : pendingQueueInit.keySet()) {
            pendingQueue.put(key, (Vector<PendingEventUID>) pendingQueueInit.get(key).clone());
        }
    }

    public void cloneMetadata(VersionVector metadataInit) {
        metadata = new VersionVector(metadataInit);
    }

    public void cloneData(VersionVector dataInit) {
        data = new VersionVector(dataInit);
    }


    private void clonePendingClients(Map<UUID, Client> pendingClientsQueueInit) {
        pendingClientsQueue = new HashMap<>();
        for (UUID uuid : pendingClientsQueueInit.keySet()) {
            pendingClientsQueue.put(uuid, pendingClientsQueueInit.get(uuid));
        }
    }


    //--------------------------------------------------------------------------
    // Delivered remote methods
    //--------------------------------------------------------------------------

    public void cloneDeliveredRemoteReads(Set<String> delivered) {
        deliveredRemoteReads = delivered;
    }

    public boolean isAlreadyDelivered(EventUID event) {
        return deliveredRemoteReads.contains(event.getKey() + "," + event.getTimestamp());
    }

    public void addRemoteRead(EventUID event) {
        deliveredRemoteReads.add(event.getKey() + "," + event.getTimestamp());
    }

    //--------------------------------------------------------------------------
    // Statistics methods
    //--------------------------------------------------------------------------
    public void cloneStatistics(int localReads, int remoteReads, int updates, int full, int partial) {
        this.localReads = localReads;
        this.remoteReads = remoteReads;
        this.updates = updates;
        this.fullMetadata = full;
        this.partialMetadata = partial;
    }

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
    public void cloneClients(List<Client> clientsInit) {
        clients = new ArrayList<>();
        for (Client client : clientsInit) {
            clients.add(client.clone());
        }
    }

    public void setClientList(Set<Integer> clientList, Map<Integer, TreeSet<Integer>> friends, int keysPerNode) {
        for (int id : clientList) {
            Client client = new Client(id, replicationGroups.size(), keysPerNode, friends.get(id));
            clients.add(client);
        }
    }

    public void setClientsCycle(int clientsCycle) {
        this.clientsCycle = clientsCycle;
    }

    public int getClientsCycle() {
        return clientsCycle;
    }

    //--------------------------------------------------------------------------
    // Friends methods
    //--------------------------------------------------------------------------
	/*public void cloneFriends(TreeSet<Integer> friendsInit){
		friends = new TreeSet<Integer>();
		Iterator<Integer> itFriends = friendsInit.iterator();
		while (itFriends.hasNext()){
			friends.add(new Integer(itFriends.next()));
		}
	}
	
	public void setFriendList(TreeSet<Integer> friendsList){
		Iterator<Integer> itFriends = friendsList.iterator();
		while (itFriends.hasNext()){
			Integer friend = new Integer(itFriends.next());
			if (!friends.contains(friend)){
				friends.add(friend);
			}
		}
	}*/
    //--------------------------------------------------------------------------
    // Replication groups methods
    //--------------------------------------------------------------------------

    /* In this case I want to use the same dictionary object for all nodes
     * thats why I simply assign the same reference instead of cloning the
     * object. Dictionary is read only, thats basically the reasoning behind
     */
    public void cloneReplicationGroups(Map<Integer, List<Integer>> rgInit) {
        replicationGroups = rgInit;
    }

    public void setReplicationGroups(Map<Integer, List<Integer>> rgInit) {
        replicationGroups = rgInit;
    }

    public List<Integer> getReplicationGroup(Integer key) {
        return replicationGroups.get(key);
    }

    public boolean isInterested(long node, long key) {
        if (replicationGroups.containsKey(Math.toIntExact(key))) {
            List<Integer> list = replicationGroups.get(Math.toIntExact(key));
            return list.contains(Math.toIntExact(node));
        } else {
            return false;
        }
    }

    //--------------------------------------------------------------------------
    // Queue methods
    //--------------------------------------------------------------------------

    @Override
    public void initQueue(Node node) {

        Linkable linkn = (Linkable) node.getProtocol(link);
        for (int i = 0; i < linkn.degree(); i++) {
            Node neighbor = linkn.getNeighbor(i);
            queue.put(neighbor.getID(), new Vector<>());
        }
    }

    public void addToQueue(EventUID event, Long from) {
        for (Long key : queue.keySet()) {
            if (key != from) {
                queue.get(key).add(event);
            }
        }
    }

    @Override
    public Vector<EventUID> getQueue(Long node) {
        return queue.get(node);
    }

    public void cleanQueue(Long node) {
        if (queue.containsKey(node)) {
            queue.get(node).clear();
        }
    }

    public void processQueue(Vector<EventUID> queue, long id) {
        for (EventUID event : queue) {
            if (event.isMigration() && id == event.getMigrationTarget()) {
                acceptClient(event.getIdentifier());
            } else if (isInterested(id, event.getKey())) {
                addMetadata(event);
            }
        }
    }

    public void acceptClient(UUID identifier) {
        Client client = pendingClientsQueue.get(identifier);
        clients.add(client);
        receivedMigrations++;
        pendingClientsQueue.remove(identifier);
        //System.out.println("Migration sucessful! " + key);
    }

    public void addQueueToQueue(Vector<EventUID> queue, Long from) {
        for (EventUID eventUID : queue) {
            this.addToQueue(eventUID, from);
        }
    }

    //--------------------------------------------------------------------------
    // Pending queue methods
    //--------------------------------------------------------------------------

    public void addToPendingQueue(EventUID event, int epoch, long senderId) {
        //System.out.println("Adding event: "+event.toString()+" to node "+senderId+" pending queue");
        if (pendingQueue.containsKey(epoch)) {
            pendingQueue.get(epoch).add(new PendingEventUID(event, senderId));
        } else {
            Vector<PendingEventUID> v = new Vector<>();
            pendingQueue.put(epoch, v);
            v.add(new PendingEventUID(event, senderId));
        }
    }

    public Vector<PendingEventUID> getPendingQueue(int epoch) {
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

    public void migrateClientQueue(Client client, EventUID label) {
        pendingClientsQueue.put(label.getIdentifier(), client);
    }

    public void addQueueToPendingQueue(Vector<EventUID> queue2, int epoch, long senderId) {
        for (EventUID eventUID : queue2) {
            this.addToPendingQueue(eventUID, epoch, senderId);
        }
    }

    @Override
    public void processPendingEpoch(int currentEpoch) {
        for (int epoch : pendingQueue.keySet()) {
            if (epoch < currentEpoch) {
                Vector<PendingEventUID> v = getPendingQueue(epoch);
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

    //--------------------------------------------------------------------------
    // Processed methods
    //--------------------------------------------------------------------------

    public int getCountProcessed() {
        return this.countProcessed;
    }

    public void setCountProcessed(int c) {
        this.countProcessed = c;
    }

    public void addProcessedEvent(EventUID event) {
        //System.out.println("Processing event "+event.getKey()+","+event.getTimestamp()+" from: "+event.getSrc()+" to: "+event.getDst()+" with latency: "+event.getLatency()+
        //		" tool "+(getEpoch() - event.getEpoch())+" cycles");
        if (event.getRemoteRead()) {
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

    public void cleanProcessed() {
        this.processed.clear();
    }

    public Vector<EventUID> getProcessed() {
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
            if (data.seenEvent(event.getKey(), event.getTimestamp())) {
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
        averageLatency = averageLatency + event.getLatency();
        data.addEvent(event.getKey(), event.getTimestamp());
        boolean matches = true;
        while (!metadataQueue.isEmpty() && (matches)) {
            EventUID head = metadataQueue.peek();
            if (data.seenEvent(head.getKey(), head.getTimestamp())) {
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
}