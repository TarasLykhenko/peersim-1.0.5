package example.common;

public class Settings {

    public static final int CLIENT_REQUEST_LATENCY = 10;
    public static final int CLIENT_MIGRATION_LATENCY = 10;
    public static final int MIN_DELAY = 5;
    public static final int MAX_DELAY = 10;

    // How long it takes for a client to connect to  other DC
    // CLIENT SETTINGS

    public static final int CLIENTS_PER_DATACENTER = 200;

    // REST_TIME = intervalo entre pedidos
    // REST_TIME_INTERVAL = variancia do intervalo

    public static final int REST_TIME = 500;
    public static final int REST_TIME_INTERVAL = 50;
    public static final int CLIENT_READ_PERCENTAGE = 80;
    public static final int CLIENT_UPDATE_PERCENTAGE = 20;

    // percentagem de clientes que nao usa restime
    public static final int CLIENT_EAGER_PERCENTAGE = 100;

    public static final String CLIENT_MIGRATION_ODDS = "[50,30,15,5]";

    public static final String CLIENT_OBJECT_READ_LVL_0 = "[50,25,15,10]";
    public static final String CLIENT_OBJECT_READ_LVL_1 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_2 = "[80,20,0,0]";
    public static final String CLIENT_OBJECT_READ_LVL_3 = "[70,20,10,0]";

    public static final String CLIENT_OBJECT_UPDATE_LVL_0 = "[50,25,15,10]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_1 = "[100,0,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_2 = "[80,20,0,0]";
    public static final String CLIENT_OBJECT_UPDATE_LVL_3 = "[70,20,10,0]";

    public static final int TOTAL_OBJECTS_PER_DATACENTER = 100;

    // Distribuicao dos objectos em cada datacenter
    public static final String LEVELS_PERCENTAGE = "[50,25,15,10]";

    public static final float STATISTICS_WINDOW = 1;

    public static final boolean SHOULD_PARTITION_DC = true;
    public static final boolean SHOULD_PARTITION_CLIENTS = false;

    // Partition lvl can be 1, 2, or 3. Any other value will load the file
    // eight_nodes_partition_custom (for projects without tree)
    // or eight_nodes_seven_brokers_partition_custom (for projects with tree)
    // Partition lvl 1: Servers cannot comunicate with other servers
    // Partition lvl 2: Each server can communicate with 1 server
    // Partition lvl 3: Each server can communicate with 3 servers
    public static final int DC_PARTITION_LEVEL = 3;
    public static final int CLIENTS_PARTITION_LEVEL = 3;


    public static final double PARTITION_START_PERCENTAGE = 20;
    public static final double PARTITION_STOP_PERCENTAGE = 70;

    // Luis Configs
    public static final boolean PARTITIONS_ARE_DELAYS = true;
    public static final double PARTITION_STRETCH_L1_PERCENTAGE = 20;
    public static final double PARTITION_STRETCH_L2_PERCENTAGE = 30;
    public static final double PARTITION_STRETCH_L3_PERCENTAGE = 40;
    public static final long PARTITION_MESSAGE_L1_AFFECTED_PERCENTAGE = 20;
    public static final long PARTITION_MESSAGE_L2_AFFECTED_PERCENTAGE = 30;
    public static final long PARTITION_MESSAGE_L3_AFFECTED_PERCENTAGE = 40;


    public static final boolean PRINT_INFO = false;
    public static final boolean PRINT_VERBOSE = false;



    // OLD SETTINGS - USED ONLY FOR OLD CLIENT - DO NOT TOUCH
    public static final String CLIENT_LOCALITY_PERCENTAGE = "[0,0,0,100]";
    public static final String CLIENT_READ_LEVEL_PERCENTAGE = "[50,25,15,10]";
    public static final String CLIENT_UPDATE_LEVEL_PERCENTAGE = "[50,25,15,10]";
}
