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
import example.saturn.datatypes.message.types.*;
import javafx.util.Pair;
import org.apache.commons.math3.distribution.ZipfDistribution;
import peersim.core.Protocol;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

import static example.common.Settings.BANDWIDTH;

public abstract class StateTreeProtocolInstance
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
    private ZipfDistribution zp;

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

    private Map<Integer, Long> keyToDataObject = new HashMap<>();

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

    Broker broker;
    ReplicationManager replicationManager;
    Storage storage;
    int usedBandwidthUp = 0;
    int usedBandwidthDown = 0;



    public long localUpdateMessage(LocalUpdateMessage message){
        GlobalContext.newNodeLogEntry((int)getNodeId(), " local update from client " + message.getClientId() ); //Log
        long value = storage.get(message.getKey());
        long newVersion = value++;
        storage.put(message.getKey(), newVersion);
        return newVersion;
    }

    public long readMessage(ReadMessage message){
        return storage.get(message.getKey());
    }

    public void metadataMessage(MetadataMessage message){
        GlobalContext.newNodeLogEntry((int)getNodeId(), " i receive  MetadataID " + message.getUpdateID() + " from " + message.getNodeOriginID()  ); //Log
        broker.newRemoteMetadataUpdate(message);

    }

    public void remoteUpdateMessage(RemoteUpdateMessage message){
        GlobalContext.newNodeLogEntry((int)getNodeId(), " i receive  DataID " + message.getUpdateID() + " from " + message.getNodeOriginID()  ); //Log
        storage.remotePut(message.getUpdateID(), message.getKey(), message.getValue());

    }


    public void addChild(StateTreeProtocolInstance child){
        broker.addChild(child);
    }

    public HashMap<Long, StateTreeProtocolInstance> getChildren(){
        return broker.getChildren();
    }

    public StateTreeProtocolInstance getParent(){
        return broker.getParent();
    }

    public void setParent(StateTreeProtocolInstance parent){
        broker.setParent(parent);
    }

    public Message getParallelMessage(){
        return replicationManager.getMessage();
    }

    public Message getFIFOMessage(){
        return broker.getMessage();
    }



    //--------------------------------------------------------------------------
    //Initialization
    //--------------------------------------------------------------------------

    /**
     * Does nothing.
     */
    public StateTreeProtocolInstance() {
        broker = new Broker();
        replicationManager = new ReplicationManager(broker);
        storage = new Storage(replicationManager);
        broker.setStorage(storage);
    }

    //--------------------------------------------------------------------------

    /**
     * Clones the value holder.
     */
    public Object clone() {
        return null;
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

        broker.setNodeID(this.nodeId);
        replicationManager.setNodeID(this.nodeId);

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

    public int get(Integer key) {
        return 0;
    }

    public int put(Integer key, Long value) {
        return 0;
    }

    public void putRemote(Integer key, Integer version) {

    }

    public int chooseRandomDataObject() {

        if(zp == null){
            zp = new ZipfDistribution(storage.kvStore.size(), 0.99);
        }
        return zp.sample() - 1;

    }

    public void generateKeys(int totalDataObjects) {

        for (int i = 0; i < totalDataObjects; i++){
            if(!GlobalContext.keysToDcs.containsKey(i)){
                GlobalContext.keysToDcs.put(i,new ArrayList<>());
            }
            GlobalContext.keysToDcs.get(i).add(nodeId);
            storage.addKey(i,0L);
        }
    }

    public int getUsedBandwidthUp(){
        return usedBandwidthUp;
    }

    public int getUsedBandwidthDown(){
        return usedBandwidthDown;
    }

    public void useBandwidthUp(int messageSize){
        usedBandwidthUp += messageSize;
    }

    public void useBandwidthDown(int messageSize){
        usedBandwidthDown += messageSize;

    }

    public void updateUsedBandwidth(){
        if((usedBandwidthUp -= BANDWIDTH) < 0){
            usedBandwidthUp = 0;
        }
        if((usedBandwidthDown -= BANDWIDTH) < 0){
            usedBandwidthDown = 0;
        }
    }
}