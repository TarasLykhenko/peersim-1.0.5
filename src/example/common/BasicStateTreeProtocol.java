package example.common;

import java.util.Set;

public interface BasicStateTreeProtocol {

    Set<? extends BasicClientInterface> getClients();
    //--------------------------------------------------------------------------
    //Statistics
    //--------------------------------------------------------------------------

    void incrementUpdates();

    void incrementRemoteReads();

    void incrementLocalReads();

    int getNumberUpdates();

    int getNumberRemoteReads();

    int getNumberLocalReads();

    void addNewReadCompleted(long timeToComplete);

    void addNewUpdateCompleted(long timeToComplete);

    long getAverageReadLatency();

    long getAverageUpdateLatency();

    void setNodeId(Long nodeId);

    long getNodeId();

    int getQueuedClients();
}
