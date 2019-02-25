package example.common;

public class Settings {

    public static final int CLIENT_REQUEST_LATENCY = 10;
    public static final int CLIENT_MIGRATION_LATENCY = 10;
    public static final int MIN_DELAY = 5;
    public static final int MAX_DELAY = 10;

    // How long it takes for a client to connect to other DC
    public static final int CLIENTS_PER_DATACENTER = 250;
    public static final int REST_TIME = 500;
    public static final int REST_TIME_INTERVAL = 50;

    public static final int CLIENT_READ_PERCENTAGE = 95;
    public static final int CLIENT_UPDATE_PERCENTAGE = 5;

    public static final String CLIENT_READ_LEVEL_PERCENTAGE = "[100,0,0]";
    public static final String CLIENT_UPDATE_LEVEL_PERCENTAGE = "[100,0,0]";

    /**
     *  percentage clients that use objects up to a given distance
     *  eg. with [50,50], half clients will only use objects from the local DC
     *  and the other half will use objects from both local and the DCs 1 level away
     */
    public static final String CLIENT_LOCALITY_PERCENTAGE = "[0,0,100]";

    public static final int CLIENT_EAGER_PERCENTAGE = 0;
    public static final int TOTAL_OBJECTS_PER_DATACENTER = 300;
    public static final String LEVELS_PERCENTAGE = "[33,33,34]";

    public static final float STATISTICS_WINDOW = 1;

    public static final boolean SHOULD_PARTITION_DC = true;
    public static final boolean SHOULD_PARTITION_CLIENTS = false;
    public static final double PARTITION_START_PERCENTAGE = 20;
    public static final double PARTITION_STOP_PERCENTAGE = 80;

    public static final boolean PRINT_IMPORTANT = true;
    public static final boolean PRINT_VERBOSE = true;

}