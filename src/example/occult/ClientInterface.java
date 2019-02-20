package example.occult;

import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.Operation;
import example.occult.no_compression.Client;

public interface ClientInterface {
    int getId();

    boolean isWaiting();

    Operation nextOperation();

    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    void receiveReadResult(long server, OccultReadResult readResult);

    void receiveUpdateResult(Integer shardId, Integer updateShardStamp);

    void migrationOver();

    //------------------------------------------
    // ------------ CLIENT STATISTICS ----------
    //------------------------------------------

    int getNumberReads();

    int getNumberUpdates();

    int getNumberMigrations();

    int getNumberMasterMigrations();

    int getNumberStaleReads();

    int getLocality();

    long getWaitingSince();

    float getAverageReadLatency();

    float getAverageUpdateLatency();

}
