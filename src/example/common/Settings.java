package example.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Settings {

    private Settings() {

    }

    private static Properties prop = new Properties();
    static {
        try (InputStream input = new FileInputStream("example/settings.txt")) {
            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(-2);
        }
    }


    public static final int CLIENT_REQUEST_LATENCY = Integer.parseInt(prop.getProperty("CLIENT_REQUEST_LATENCY"));
    public static final float CLIENT_NETWORK_STRETCH = Float.parseFloat(prop.getProperty("CLIENT_NETWORK_STRETCH"));
    public static final int MIN_DELAY = Integer.parseInt(prop.getProperty("MIN_DELAY"));
    public static final int MAX_DELAY = Integer.parseInt(prop.getProperty("MAX_DELAY"));

    public static final int VALUE_SIZE = Integer.parseInt(prop.getProperty("VALUE_SIZE"));

    public static final int BANDWIDTH = Integer.parseInt(prop.getProperty("BANDWIDTH"));



    // How long it takes for a client to connect to  other DC
    // CLIENT SETTINGS

    public static final int CLIENTS_PER_DATACENTER = Integer.parseInt(prop.getProperty("CLIENTS_PER_DATACENTER"));

    // REST_TIME = intervalo entre pedidos
    // REST_TIME_INTERVAL = variancia do intervalo

    public static final int REST_TIME = Integer.parseInt(prop.getProperty("REST_TIME"));
    public static final int REST_TIME_INTERVAL = Integer.parseInt(prop.getProperty("REST_TIME_INTERVAL"));
    public static final int CLIENT_READ_PERCENTAGE = Integer.parseInt(prop.getProperty("CLIENT_READ_PERCENTAGE"));
    public static final int CLIENT_UPDATE_PERCENTAGE = Integer.parseInt(prop.getProperty("CLIENT_UPDATE_PERCENTAGE"));

    // percentagem de clientes que nao usa restime
    public static final int CLIENT_EAGER_PERCENTAGE = Integer.parseInt(prop.getProperty("CLIENT_EAGER_PERCENTAGE"));

    public static final String CLIENT_MIGRATION_ODDS = prop.getProperty("CLIENT_MIGRATION_ODDS");

    public static final String CLIENT_OBJECT_READ_LVL_0 = prop.getProperty("CLIENT_OBJECT_READ_LVL_0");
    public static final String CLIENT_OBJECT_READ_LVL_1 = prop.getProperty("CLIENT_OBJECT_READ_LVL_1");
    public static final String CLIENT_OBJECT_READ_LVL_2 = prop.getProperty("CLIENT_OBJECT_READ_LVL_2");
    public static final String CLIENT_OBJECT_READ_LVL_3 = prop.getProperty("CLIENT_OBJECT_READ_LVL_3");

    public static final String CLIENT_OBJECT_UPDATE_LVL_0 = prop.getProperty("CLIENT_OBJECT_UPDATE_LVL_0");
    public static final String CLIENT_OBJECT_UPDATE_LVL_1 = prop.getProperty("CLIENT_OBJECT_UPDATE_LVL_1");
    public static final String CLIENT_OBJECT_UPDATE_LVL_2 = prop.getProperty("CLIENT_OBJECT_UPDATE_LVL_2");
    public static final String CLIENT_OBJECT_UPDATE_LVL_3 = prop.getProperty("CLIENT_OBJECT_UPDATE_LVL_3");

    public static final int TOTAL_OBJECTS_PER_DATACENTER = 1000;

    // Distribuicao dos objectos em cada datacenter
    public static final String LEVELS_PERCENTAGE = prop.getProperty("LEVELS_PERCENTAGE");

    public static final float STATISTICS_WINDOW = Float.parseFloat(prop.getProperty("STATISTICS_WINDOW"));

    public static final boolean SHOULD_PARTITION_DC = Boolean.parseBoolean(prop.getProperty("SHOULD_PARTITION_DC"));
    public static final boolean SHOULD_PARTITION_CLIENTS = Boolean.parseBoolean(prop.getProperty("SHOULD_PARTITION_CLIENTS"));

    // Partition lvl can be 1, 2, or 3. Any other value will load the file
    // eight_nodes_partition_custom (for projects without tree)
    // or eight_nodes_seven_brokers_partition_custom (for projects with tree)
    // Partition lvl 1: Servers cannot comunicate with other servers
    // Partition lvl 2: Each server can communicate with 1 server
    // Partition lvl 3: Each server can communicate with 3 servers
    public static final int DC_PARTITION_LEVEL = Integer.parseInt(prop.getProperty("DC_PARTITION_LEVEL"));
    public static final int CLIENTS_PARTITION_LEVEL = Integer.parseInt(prop.getProperty("CLIENTS_PARTITION_LEVEL"));


    public static final double PARTITION_START_PERCENTAGE = Double.parseDouble(prop.getProperty("PARTITION_START_PERCENTAGE"));
    public static final double PARTITION_STOP_PERCENTAGE = Double.parseDouble(prop.getProperty("PARTITION_STOP_PERCENTAGE"));

    // Luis Configs
    public static final boolean PARTITIONS_ARE_DELAYS = Boolean.parseBoolean(prop.getProperty("PARTITIONS_ARE_DELAYS"));
    public static final double PARTITION_STRETCH_L1_PERCENTAGE = Double.parseDouble(prop.getProperty("PARTITION_STRETCH_L1_PERCENTAGE"));
    public static final double PARTITION_STRETCH_L2_PERCENTAGE = Double.parseDouble(prop.getProperty("PARTITION_STRETCH_L2_PERCENTAGE"));
    public static final double PARTITION_STRETCH_L3_PERCENTAGE = Double.parseDouble(prop.getProperty("PARTITION_STRETCH_L3_PERCENTAGE"));
    public static final long PARTITION_MESSAGE_L1_AFFECTED_PERCENTAGE = Long.parseLong(prop.getProperty("PARTITION_MESSAGE_L1_AFFECTED_PERCENTAGE"));
    public static final long PARTITION_MESSAGE_L2_AFFECTED_PERCENTAGE = Long.parseLong(prop.getProperty("PARTITION_MESSAGE_L2_AFFECTED_PERCENTAGE"));
    public static final long PARTITION_MESSAGE_L3_AFFECTED_PERCENTAGE = Long.parseLong(prop.getProperty("PARTITION_MESSAGE_L3_AFFECTED_PERCENTAGE"));


    public static final String TOPOLOGY_FILE = prop.getProperty("TOPOLOGY_FILE");
    public static final String METRICS = prop.getProperty("METRICS");

    // DEBUG
    public static final boolean PRINT_INFO = true;
    public static final boolean PRINT_CYCLE = false;

    public static final boolean PRINT_VERBOSE = true;
    public static final String CLIENT_LOCALITY_PERCENTAGE = "[0,0,0,100]";
    public static final String CLIENT_READ_LEVEL_PERCENTAGE = "[50,25,15,10]";
    public static final String CLIENT_UPDATE_LEVEL_PERCENTAGE = "[50,25,15,10]";
}
