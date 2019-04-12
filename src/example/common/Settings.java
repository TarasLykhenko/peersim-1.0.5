package example.common;

public class Settings {

    public static final int CLIENT_REQUEST_LATENCY = 10;
    public static final int CLIENT_MIGRATION_LATENCY = 10;
    public static final int MIN_DELAY = 5;
    public static final int MAX_DELAY = 10;

    // How long it takes for a client to connect to other DC
    // CLIENT SETTINGS

    public static final int CLIENTS_PER_DATACENTER = 200;
    public static final int REST_TIME = 500;
    public static final int REST_TIME_INTERVAL = 50;
    public static final int CLIENT_READ_PERCENTAGE = 95;
    public static final int CLIENT_UPDATE_PERCENTAGE = 5;
    public static final int CLIENT_EAGER_PERCENTAGE = 0;

    public static final String CLIENT_MIGRATION_ODDS = "[40,20,20,20]";

    public static final String CLIENT_OBJECT_READ_LVL_0 = "[80,10,5,5]";
    public static final String CLIENT_OBJECT_READ_LVL_1 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_2 = "[90,10,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_3 = "[40,20,40,0]";

    public static final String CLIENT_OBJECT_UPDATE_LVL_0 = "[80,10,5,5]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_1 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_2 = "[90,10,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_3 = "[40,20,40,0]";

    public static final int TOTAL_OBJECTS_PER_DATACENTER = 50;
    public static final String LEVELS_PERCENTAGE = "[50,25,15,10]";

    public static final float STATISTICS_WINDOW = 1;

    public static final boolean SHOULD_PARTITION_DC = true;
    public static final boolean SHOULD_PARTITION_CLIENTS = false;
    public static final double PARTITION_START_PERCENTAGE = 20;
    public static final double PARTITION_STOP_PERCENTAGE = 80;

    public static final boolean PRINT_INFO = true;
    public static final boolean PRINT_IMPORTANT = true;
    public static final boolean PRINT_VERBOSE = false;



    // OLD SETTINGS - USED ONLY FOR OLD CLIENT
    public static final String CLIENT_LOCALITY_PERCENTAGE = "[0,0,0,100]";
    public static final String CLIENT_READ_LEVEL_PERCENTAGE = "[50,25,15,10]";
    public static final String CLIENT_UPDATE_LEVEL_PERCENTAGE = "[50,25,15,10]";
}