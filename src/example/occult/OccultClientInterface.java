package example.occult;

import example.common.BasicClientInterface;
import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.Operation;
import example.occult.no_compression.Client;

public interface OccultClientInterface extends BasicClientInterface {

    Operation nextOperation();

    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    void receiveReadResult(long server, OccultReadResult readResult);

    void receiveUpdateResult(Integer shardId, Integer updateShardStamp);

    int getNumberMasterMigrations();

    int getNumberCatchAll();

    int getNumberStaleReads();
}
