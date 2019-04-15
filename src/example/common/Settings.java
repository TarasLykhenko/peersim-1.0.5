package example.common;

public class Settings {

    public static final int CLIENT_REQUEST_LATENCY = 10;
    public static final int CLIENT_MIGRATION_LATENCY = 10;
    public static final int MIN_DELAY = 5;
    public static final int MAX_DELAY = 10;

    // How long it takes for a client to connect to other DC
    // CLIENT SETTINGS

    public static final int CLIENTS_PER_DATACENTER = 200;

    // REST_TIME = intervalo entre pedidos
    // REST_TIME_INTERVAL = variancia do intervalo

    public static final int REST_TIME = 500;
    public static final int REST_TIME_INTERVAL = 50;
    public static final int CLIENT_READ_PERCENTAGE = 90;
    public static final int CLIENT_UPDATE_PERCENTAGE = 10;

    // percentagem de clientes que nao usa restime
    public static final int CLIENT_EAGER_PERCENTAGE = 100;

    public static final String CLIENT_MIGRATION_ODDS = "[50,50,0,0]";

    public static final String CLIENT_OBJECT_READ_LVL_0 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_1 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_2 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_3 = "[100,0,0,0]";

    public static final String CLIENT_OBJECT_UPDATE_LVL_0 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_1 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_2 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_3 = "[100,0,0,0]";

    public static final int TOTAL_OBJECTS_PER_DATACENTER = 50;

    // Distribuicao dos objectos em cada datacenter
    public static final String LEVELS_PERCENTAGE = "[50,25,15,10]";

    public static final float STATISTICS_WINDOW = 1;

    public static final boolean SHOULD_PARTITION_DC = false;
    public static final boolean SHOULD_PARTITION_CLIENTS = true;

    // Partition lvl can be 1, 2, or 3. Any other value will load the file
    // eight_nodes_partition_custom (for projects without tree)
    // or eight_nodes_seven_brokers_partition_custom (for projects with tree)
    // Partition lvl 1: Servers cannot comunicate with other servers
    // Partition lvl 2: Each server can communicate with 1 server
    // Partition lvl 3: Each server can communicate with 3 servers
    public static final int DC_PARTITION_LEVEL = 1;
    public static final int CLIENTS_PARTITION_LEVEL = 1;


    public static final double PARTITION_START_PERCENTAGE = 20;
    public static final double PARTITION_STOP_PERCENTAGE = 70;

    public static final boolean PRINT_INFO = false;
    public static final boolean PRINT_VERBOSE = false;



    // OLD SETTINGS - USED ONLY FOR OLD CLIENT - DO NOT TOUCH
    public static final String CLIENT_LOCALITY_PERCENTAGE = "[0,0,0,100]";
    public static final String CLIENT_READ_LEVEL_PERCENTAGE = "[50,25,15,10]";
    public static final String CLIENT_UPDATE_LEVEL_PERCENTAGE = "[50,25,15,10]";
}
