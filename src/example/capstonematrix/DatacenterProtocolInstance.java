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

package example.capstonematrix;

import example.capstonematrix.datatypes.HRC;
import example.capstonematrix.datatypes.ReadResult;
import example.capstonematrix.datatypes.UpdateMessage;
import example.capstonematrix.datatypes.UpdateResult;
import example.common.BasicClientInterface;
import example.common.Settings;
import example.common.datatypes.DataObject;
import javafx.util.Pair;
import peersim.core.Network;
import peersim.core.Protocol;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class DatacenterProtocolInstance
        implements StateTreeProtocol, Protocol {

    //--------------------------------------------------------------------------
    //Fields
    //--------------------------------------------------------------------------

    private final int tree = 2; //This is very hardcoded cba.

    protected int localReads = 0;
    protected int remoteReads = 0;
    protected int updates = 0;


    protected Set<Client> clients = new HashSet<>();
    protected Map<Integer, Client> idToClient = new HashMap<>();

    protected long nodeId;
    protected int regionId;

    /**
     * Cloudlet Logical Clock
     */
    protected int cloudletLC;

    /**
     * Stores the last operationValue from each cloudlet
     *
     */
    private Map<Long, Integer> lastReceived = new HashMap<>();

    /**
     * Migration Table:
     * Maps each datacenter to a map of clients to their past (HRC)
     *
     * Clients that cannot be immediately accepted are put on this table,
     * on the correct datacenter entry.
     *
     * NOTE: This past is already transformed.
     */
    private Map<Long, Map<Client, HRC>> migrationTable = new HashMap<>();

    /**
     * Buffers the remote updates of each datacenter
     *
     * Map is datacenterId to an orderedList of buffered Updates
     */
    private Map<Long, List<BufferedUpdate>> remoteUpdatesTable = new HashMap<>();

    /**
     * Stores each key alongside a HRC.
     *
     * Map is dataObjectKey to HRC
     */
    private Map<Integer, HRC> storageTable = new HashMap<>();

    private Map<Integer, DataObject> keyToDataObject = new HashMap<>();

    protected long sentMigrations = 0;
    private long receivedMigrations = 0;

    private Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
    private Map<Integer, Set<DataObject>> levelToDataObjects = new HashMap<>();
    private Set<DataObject> allDataObjects = new HashSet<>();


    //--------------------------------------------------------------------------
    //Initialization
    //--------------------------------------------------------------------------

    /**
     * Does nothing.
     */
    DatacenterProtocolInstance(String prefix) {

    }

    public void init() {
        this.regionId = GroupsManager.getInstance().getMostSpecificRegion(nodeId);
        initLastReceived();
        initRUT();
        initMT();
        initStorageTable();
    }

    @Override
    public void setNodeId(Long nodeId) {
        this.nodeId = Math.toIntExact(nodeId);
    }

    private void initLastReceived() {
        for (long datacenterId = 0; datacenterId < Network.size(); datacenterId++) {
            updateLastReceived(datacenterId, 0);
        }
    }

    private void initRUT() {
        for (long datacenterId = 0; datacenterId < Network.size(); datacenterId++) {
            remoteUpdatesTable.put(datacenterId, new ArrayList<>());
        }
    }

    private void initMT() {
        for (long datacenterId = 0; datacenterId < Network.size(); datacenterId++) {
            migrationTable.put(datacenterId, new HashMap<>());
        }
    }

    private void initStorageTable() {
        for (int key : storageTable.keySet()) {
            capstoneWrite(key, new HRC(regionId));
        }
    }


    //--------------------------------------------------------------------------
    // Delivered remote methods

    //--------------------------------------------------------------------------

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
            capstoneWrite(dataObject.getKey(), null);
        }
    }

    @Override
    public Set<DataObject> getDataObjectsFromLevel(int level) {
        return levelToDataObjects.get(level);
    }
    //--------------------------------------------------------------------------
    // Statistics methods

    //--------------------------------------------------------------------------

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
        Set<Client> result = new HashSet<>(clients);
        for (Long datacenter : migrationTable.keySet()) {
            result.addAll(migrationTable.get(datacenter).keySet());
        }
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
    public void addClients(Set<Client> clientList) {
        clients.addAll(clientList);
        for (Client client : clientList) {
            idToClient.put(client.getId(), client);
        }
    }


    //--------------------------------------------------------------------------
    // Replication groups methods
    //--------------------------------------------------------------------------

    public boolean isInterested(int key) {
        return keyToDataObject.containsKey(key);
    }

    //--------------------------------------------------------------------------
    // PUBLIC SERVICE METHODS
    //--------------------------------------------------------------------------

    /**
     * Returns the localDataClock of key.
     */
    ReadResult capstoneRead(int key) {
        return new ReadResult(storageTable.get(key));
    }

    /**
     * Here we don't return anything as all logic is done on the frontend
     * @param key
     * @param incorporatedHRC
     * @return
     */
    void capstoneWrite(int key, HRC incorporatedHRC) {
        if (incorporatedHRC != null) {
            if (Settings.PRINT_INFO) {
                System.out.println("Writing HRC of region " + incorporatedHRC.getRegionId());
                incorporatedHRC.print();
            }
        }
        storageTable.put(key, incorporatedHRC);
    }

    /**
     * Algorithm for when receiving a remote update:
     *
     * 1) Update RUT
     * 2) Update own CloudletClock
     * 3) Update ST with updated CloudletClock
     * 4) Check MT for any clients that can be accepted
     */
    void processRemoteUpdate(UpdateMessage updateMessage) {
        long sourceId = updateMessage.getOriginalDC();
        int key = updateMessage.getKey();
        int updateLC = updateMessage.getUpdateLC();
        HRC past = updateMessage.getPast();

        if (Settings.PRINT_INFO) {
            System.out.println("(MSG " + updateLC + ") Original HRC: (Node " + sourceId + " to " + nodeId + ")");
            past.print();
        }
        HRC transformedHRC = past.transform((int) sourceId, (int) nodeId);

        if (Settings.PRINT_INFO) {
            System.out.println("Transformed HRC:");
            transformedHRC.print();
        }

        if (transformedHRC.canAcceptHRC(sourceId, nodeId, lastReceived)) {
            applyUpdate(key, sourceId, updateLC, transformedHRC);
        } else {
            //System.exit(-1);
            if (Settings.PRINT_INFO) {
                System.out.println("Buffering!");
            }
            bufferRemoteUpdate(key, sourceId, updateLC, transformedHRC);
        }
    }

    private void bufferRemoteUpdate(int key, long sourceId, int updateLC, HRC transformedHRC) {
        BufferedUpdate bufferedUpdate = new BufferedUpdate(key, updateLC, transformedHRC);

        remoteUpdatesTable.get(sourceId).add(bufferedUpdate);
    }

    private void applyUpdate(int key, long sourceId, int updateLC, HRC hrc) {
        updateLastReceived(sourceId, updateLC);
        HRC incorporated = hrc.incorporate(key, sourceId, updateLC);
        HRC transformedIncorporated = incorporated.transform((int) sourceId, (int) nodeId);

        capstoneWrite(key, transformedIncorporated);

        checkMigrationTable(sourceId);
        checkRemoteUpdatesTable();
    }

    protected void updateLastReceived(long sourceId, int updateLc) {
        lastReceived.put(sourceId, updateLc);
        if (updateLc > cloudletLC) {
            cloudletLC = updateLc;
            lastReceived.put(nodeId, cloudletLC);
        }
    }

    protected void checkRemoteUpdatesTable() {
        for (Long datacenterId : remoteUpdatesTable.keySet()) {
            List<BufferedUpdate> bufferedUpdates = remoteUpdatesTable.get(datacenterId);
            if (!bufferedUpdates.isEmpty()) {
                BufferedUpdate update = bufferedUpdates.get(0);

                if (update.past.canAcceptHRC(datacenterId, nodeId, lastReceived)) {
                    if (Settings.PRINT_INFO) {
                        System.out.println("UNBUFFERING!");
                    }
                    bufferedUpdates.remove(0);
                    applyUpdate(update.key, datacenterId, update.updateLC, update.past);
                    return;
                }
            }
        }
    }

    void migrateClient(long originDatacenter,
                       Client client,
                       HRC migrationClock) {

        HRC transformedHRC = migrationClock.transform((int) originDatacenter, (int) nodeId);

        if (transformedHRC.canAcceptHRC(originDatacenter, nodeId, lastReceived)) {
            acceptClient(client, transformedHRC);
            client.instantMigrationAccept();
        } else {
            if (Settings.PRINT_INFO) {
                System.out.println("Could not accept client " + client.getId() + " from " + originDatacenter + " to " + nodeId);
            }
          //  System.out.println("Original client HRC:");
          //  migrationClock.print();
          //  System.out.println("Transformed HRC:");
          //  transformedHRC.print();
            migrationTable.get(originDatacenter).put(client, transformedHRC);
        }
    }

    protected void checkMigrationTable(long datacenter) {
        Map<Client, HRC> clientsToHRC = this.migrationTable.get(datacenter);
        Iterator<Client> iterator = clientsToHRC.keySet().iterator();
        while (iterator.hasNext()) {
            Client client = iterator.next();

            HRC transformedPast = clientsToHRC.get(client);

           // System.out.println("Checking if can accept: (Node " + nodeId + ")");
           // transformedPast.print();
           // System.out.println("Map: " + lastReceived);
            if (transformedPast.canAcceptHRC(datacenter, nodeId, lastReceived)) {
            //    System.out.println("Accepted client " + client.getId());
                acceptClient(client, transformedPast);
                iterator.remove();
            }
        }
    }

    private void acceptClient(Client client, HRC transformedPast) {
        clients.add(client);
        idToClient.put(client.getId(), client);
        client.migrationOver(nodeId, transformedPast);
        receivedMigrations++;
        //System.out.println("Migration sucessful! " + key);
    }

    @Override
    public int getQueuedClients() {
        int totalWaitingClients = 0;
        for (Long dc : migrationTable.keySet()) {
            totalWaitingClients += migrationTable.get(dc).size();
        }
        return totalWaitingClients;
    }

    public Map<Long, Map<Client, HRC>> getMigrationTable() {
        return migrationTable;
    }

    public Map<Long, Integer> getLastReceived() {
        return lastReceived;
    }

    //--------------------------------------------------------------------------
    // Timestamp methods
    //--------------------------------------------------------------------------

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

    @Override
    public String toString() {
        return String.valueOf(nodeId);
    }


    class BufferedUpdate {

        int key;
        int updateLC;
        HRC past;

        public BufferedUpdate(int key, int updateLC, HRC past) {
            this.key = key;
            this.updateLC = updateLC;
            this.past = past;
        }
    }
}