package example.myproject;

import example.myproject.server.BackendInterface;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Controller implements Control {


    private static final String PAR_PROT = "protocol";
    private static final String PAR_OUTPUT = "output_file";


    private int iteration = 1;
    /**
     * Protocol identifier
     */
    protected final int pid;
    private PrintWriter writer;
    private PrintWriter importantWriter;
    private int currentPoint = 0;

    private final String outputFile;
    private static final boolean WRITE_TO_SOUT = true;
    private final boolean PRINT_VERBOSE = false;
    private final boolean PRINT_IMPORTANT = false;
    private final float STATISTICS_WINDOW = 5;

    private final int takeStatisticsEvery;

    private int cycles;

    public Controller(String name) throws IOException {

        pid = Configuration.getPid(name + "." + PAR_PROT);
        outputFile = Configuration.getString(name + "." + PAR_OUTPUT);


        int endTime = Configuration.getInt("simulation.endtime");
        int logTime = Configuration.getInt("simulation.logtime");
        cycles = endTime / logTime;


        if (PRINT_VERBOSE) {
            DateFormat dateFormat = new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss");
            Calendar cal = Calendar.getInstance();
            String pathfile = outputFile + dateFormat.format(cal.getTime()) + ".txt";
            FileWriter fr = new FileWriter(pathfile, false);
            BufferedWriter br = new BufferedWriter(fr);
            writer = new PrintWriter(br);
        }

        if (PRINT_IMPORTANT) {
            String importantPathfile = outputFile + "myproject" + ".txt";
            FileWriter fr2 = new FileWriter(importantPathfile, false);
            BufferedWriter br2 = new BufferedWriter(fr2);
            importantWriter = new PrintWriter(br2);
        }

        takeStatisticsEvery = Math.round((STATISTICS_WINDOW / 100) * cycles);
    }

    public boolean execute() {
        iteration++;

        if (iteration != cycles) {
            if (iteration % takeStatisticsEvery != 0) {
                return false;
            }
        }

        currentPoint += STATISTICS_WINDOW;

        print("Observer init ======================");

        for (int i = 0; i < Network.size(); i++) {
            BackendInterface backend = (BackendInterface) Network.get(i).getProtocol(pid);
            String status = backend.printStatus();
            print(status);
        }


        print("Observer end =======================");
        print("");

        if (cycles == iteration) {
            if (PRINT_VERBOSE) {
                writer.close();
            }
            if (PRINT_IMPORTANT) {
                importantWriter.close();
            }
            return true;
        }
        return false;
    }

    protected void print(String string) {
        if (PRINT_VERBOSE) {
            writer.println(string);
        }
        if (WRITE_TO_SOUT) {
            System.out.println(string);
        }
    }

    protected void printImportant(String string) {
        if (PRINT_IMPORTANT) {
            importantWriter.println(string);
        }
    }
}
