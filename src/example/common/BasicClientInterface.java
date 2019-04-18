package example.common;

public interface BasicClientInterface {

    int getId();

    //------------------------------------------
    // ------------ CLIENT STATISTICS ----------
    //------------------------------------------

    //void nextOperation();

    int getNumberReads();

    int getNumberUpdates();

    int getNumberMigrations();

    int getLocality();

    long getWaitingSince();

    float getAverageReadLatency();

    float getAverageUpdateLatency();

    float getAverageMigrationTime();

    int getInstantMigrationsAccept();

    //------------------------------------------
    // ------------ CLIENT STATUS --------------
    //------------------------------------------

    boolean isWaiting();

    void migrationStart();

    void migrationOver(long dcId);

    void instantMigrationAccept();
}
