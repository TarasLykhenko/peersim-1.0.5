package example.occult;

import example.common.BasicClientInterface;
import example.common.datatypes.Operation;
import example.occult.datatypes.OccultReadResult;

public interface OccultClientInterface extends BasicClientInterface {

    Operation nextOperation();

    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    void occultReceiveReadResult(long server, OccultReadResult readResult);

    void occultReceiveUpdateResult(Integer shardId, Integer updateShardStamp);

    int getNumberMasterMigrations();

    int getNumberCatchAll();

    int getNumberStaleReads();
}
