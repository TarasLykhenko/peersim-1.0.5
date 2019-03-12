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

import example.capstonematrix.datatypes.ReadResult;
import example.capstonematrix.datatypes.UpdateMessage;
import example.capstonematrix.datatypes.UpdateResult;
import example.common.BasicClientInterface;
import example.common.datatypes.DataObject;
import peersim.core.Protocol;

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

    /**
     * Cloudlet clock
     */
    private Map<Long, Integer> cloudletClock = new HashMap<>();

    /**
     * Migration Table:
     * Maps each datacenter to a map of clients to ClientClocks.
     *
     * Clients that cannot be immediatly accepted are put on this table,
     * in the correct datacenter entry.
     */
    private Map<Long, Map<Client, Map<Long, Integer>>> migrationTable = new HashMap<>();

    /**
     * Stores the last update from each datacenter.
     *
     * Map is datacenterId to CloudletClock.
     */
    private Map<Long, Map<Long, Integer>> remoteUpdatesTable = new HashMap<>();

    /**
     * Stores each key alongside a localDataClock.
     *
     * Map is dataObjectKey to LocalDataClock
     */
    private Map<Integer, Map<Long, Integer>> storageTable = new HashMap<>();
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

    public void init(Long nodeId) {
        TreeOverlay treeOverlay = GroupsManager.getInstance().getTreeOverlay();
        setNodeId(nodeId);
        initCloudletClock(treeOverlay);
        initRUT(treeOverlay);
        initMT(treeOverlay);
    }

    @Override
    public void setNodeId(Long nodeId) {
        this.nodeId = Math.toIntExact(nodeId);
    }

    private void initCloudletClock(TreeOverlay treeOverlay) {
        List<Long> nodesToRoot =
                treeOverlay.getNodesOnPathToRoot(Math.toIntExact(nodeId));
        for (Long entry : nodesToRoot) {
            cloudletClock.put(entry, 0);
        }
    }

    private void initRUT(TreeOverlay treeOverlay) {
        Set<Long> datacenters = treeOverlay.getLeaves();

        for (Long datacenterId : datacenters) {
            Map<Long, Integer> remoteUpdateClock = new HashMap<>();

            List<Long> nodesToRoot =
                    treeOverlay.getNodesOnPathToRoot(Math.toIntExact(datacenterId));

            for (Long entry : nodesToRoot) {
                remoteUpdateClock.put(entry, 0);
            }
            remoteUpdatesTable.put(datacenterId, remoteUpdateClock);
        }
    }

    private void initMT(TreeOverlay treeOverlay) {
        Set<Long> datacenters = treeOverlay.getLeaves();

        for (Long datacenterId : datacenters) {
            migrationTable.put(datacenterId, new HashMap<>());
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
            storageTable.put(dataObject.getKey(), new HashMap<>(cloudletClock));
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

    UpdateResult capstoneWrite(int key, Map<Long, Integer> clientClock) {
        Map<Long, Integer> localDataClock = storageTable.get(key);

        Integer cloudletCounter = cloudletClock.get(nodeId);
        cloudletCounter++;
        cloudletClock.put(nodeId, cloudletCounter);

        clientClock.put(nodeId, cloudletCounter);

        Map<Long, Integer> maxedClock = getEntryWiseMaxClock(clientClock, localDataClock);

        storageTable.put(key, maxedClock);

        return new UpdateResult(nodeId, cloudletCounter);
    }

    /**
     * Algorithm for when receiving a remote update:
     *
     * 1) Update RUT
     * 2) Update own CloudletClock
     * 3) Update ST with updated CloudletClock
     * 4) Check MT for any clients that can be accepted
     */
    void
    processRemoteUpdate(UpdateMessage updateMessage) {
        long datacenterUpdateOrigin = updateMessage.getOriginalDC();
        int key = updateMessage.getKey();
        Map<Long, Integer> updateClock = updateMessage.getPast();

        updateRemoteUpdateTable(datacenterUpdateOrigin, updateClock);
        updateOwnCloudletClock(updateClock);
        if (isInterested(key)) {
            updateStorageTable(key);
        }
        checkMigrationTable(datacenterUpdateOrigin);
    }

    void migrateClient(long originDatacenter,
                       Client client,
                       List<Map<Long, Integer>> migrationClock) {
        Map<Long, Integer> latestDCClock = this.remoteUpdatesTable.get(originDatacenter);
        if (canAcceptClient(originDatacenter, migrationClock, nodeId, latestDCClock)) {
            acceptClient(client);
        } else {
            migrationTable.get(originDatacenter).put(client, migrationClock);
        }
    }

    private void updateRemoteUpdateTable(long datacenter, Map<Long, Integer> updateClock) {
        this.remoteUpdatesTable.put(datacenter, updateClock);
    }

    private void updateOwnCloudletClock(Map<Long, Integer> updateClock) {
        for (Long pathNodeId : updateClock.keySet()) {
            if (cloudletClock.containsKey(pathNodeId)) {
                int cloudletNodeVersion = cloudletClock.get(pathNodeId);
                int updateClockNodeVersion = updateClock.get(pathNodeId);

                if (updateClockNodeVersion > cloudletNodeVersion) {
                    cloudletClock.put(pathNodeId, updateClockNodeVersion);
                }
            }
        }
    }

    private void updateStorageTable(int key) {
        this.storageTable.put(key, cloudletClock);
    }

    private void checkMigrationTable(long datacenter) {
        Map<Client, Map<Long, Integer>> clientMapMap = this.migrationTable.get(datacenter);
        Iterator<Client> iterator = clientMapMap.keySet().iterator();
        while (iterator.hasNext()) {
            Client client = iterator.next();
            Map<Long, Integer> migrationClock = clientMapMap.get(client);
            Map<Long, Integer> latestDCClock = this.remoteUpdatesTable.get(datacenter);

            if (canAcceptClient(datacenter, migrationClock, nodeId, latestDCClock)) {
                acceptClient(client);
                iterator.remove();
            }
        }
    }

    private boolean canAcceptClient(long originDC,
                                    Map<Long, Integer> migrationClock,
                                    long thisDcId,
                                    Map<Long, Integer> latestDCClock) {
        if (migrationClock.size() != latestDCClock.size()) {
            throw new RuntimeException("Invalid size of clocks!");
        }

        TreeOverlay treeOverlay = GroupsManager.getInstance().getTreeOverlay();

        List<Long> dcToRoot = treeOverlay.getNodesOnPathToRoot(originDC);
        List<Long> thisToRoot = treeOverlay.getNodesOnPathToRoot(thisDcId);
        List<Long> interestingEntries = new ArrayList<>();

        for (int i = 0; i < dcToRoot.size(); i++) {
            Long entryOriginDC = dcToRoot.get(i);
            Long entryThisDC = thisToRoot.get(i);

            if (!entryOriginDC.equals(entryThisDC)) {
                interestingEntries.add(entryOriginDC);
            }
        }


        for (Long entryId : interestingEntries) {
            if (!latestDCClock.containsKey(entryId)) {
                throw new RuntimeException("Both clocks need to have the same entries.");
            }

            int migrationClockValue = migrationClock.get(entryId);
            int latestDCClockValue = latestDCClock.get(entryId);

            if (migrationClockValue > latestDCClockValue) {
                return false;
            }
        }

        return true;
    }


    // TODO Nota importante: Não está especificado o que acontece ao clientHRC
    private void acceptClient(Client client) {
        clients.add(client);
        idToClient.put(client.getId(), client);
        client.migrationOver(cloudletClock);
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

    static Map<Long, Integer> getEntryWiseMaxClock(Map<Long, Integer> clockOne,
                                                      Map<Long, Integer> clockTwo) {
        Map<Long, Integer> result = new HashMap<>();

        if (clockOne.size() != clockTwo.size()) {
            throw new RuntimeException("The clocks size must be equal!");
        }

        for (Long entry : clockOne.keySet()) {
            if (!clockTwo.containsKey(entry)) {
                throw new RuntimeException("Both clocks must contain the same entries.");
            }
            int entryValueOne = clockOne.get(entry);
            int entryValueTwo = clockTwo.get(entry);

            if (entryValueOne > entryValueTwo) {
                result.put(entry, entryValueOne);
            } else {
                result.put(entry, entryValueTwo);
            }
        }

        return result;
    }
}