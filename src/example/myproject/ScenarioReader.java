package example.myproject;

import example.myproject.datatypes.AssertException;
import javafx.util.Pair;
import peersim.config.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScenarioReader {

    private static ScenarioReader scenarioReader;
    private static final String PAR_SCENARIO_NAME = "scenario-name";

    // Part 1
    private String topologyType;
    private List<Pair<Integer, Integer>> connections;

    // Part 2
    public static Map<String, Integer> topicStringsToInts;
    private Map<Long, Set<Integer>> nodesToGroups;

    // Part 3
    private Map<Long, List<String>> actions;

    public static ScenarioReader getInstance() {
        if (scenarioReader == null) {
            scenarioReader = new ScenarioReader();
        }
        return scenarioReader;
    }

    private ScenarioReader() {
        String fileName = Configuration.getString(PAR_SCENARIO_NAME);
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader("example/scenarios/" + fileName));
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Could not find file example/scenarios/" + fileName);
        }

        try {
            parseBufferedReader(bufferedReader);
        } catch (ItsOverException itsOver) {
            // Nothing to catch, as it's over.
        }

        System.out.println("TESTING!");
        System.out.println("CONNECTIONS: " + connections);
        System.out.println("NODES TO GROUPS: " + nodesToGroups);
        System.out.println("ACTIONS: " + actions);
    }

    private void parseBufferedReader(BufferedReader bufferedReader) {
        String currentLine = updateNextBufReaderLine(bufferedReader);

        String separation = ">===<";
        while (!currentLine.equals(separation)) {
            handleTopologyInfo(currentLine);
            currentLine = updateNextBufReaderLine(bufferedReader);
        }

        currentLine = updateNextBufReaderLine(bufferedReader);
        while (!currentLine.equals(separation)) {
            handleGroupsInfo(currentLine);
            currentLine = updateNextBufReaderLine(bufferedReader);
        }

        currentLine = updateNextBufReaderLine(bufferedReader);
        while (!currentLine.equals(separation)) {
            handleActionsInfo(currentLine);
            currentLine = updateNextBufReaderLine(bufferedReader);
        }

    }

    private void handleTopologyInfo(String currentLine) {
        if (currentLine.equals("linear")) {
            topologyType = "linear";
            return;
        } else if (currentLine.equals("tree")) {
            topologyType = "tree";
            return;
        }

        topologyType = "file";

        if (connections == null) {
            connections = new ArrayList<>();
        }

        String[] lineTokens = currentLine.split("-");
        int firstNode = Integer.valueOf(lineTokens[0]);
        int otherNode = Integer.valueOf(lineTokens[1]);

        connections.add(new Pair<>(firstNode, otherNode));
    }

    private void handleGroupsInfo(String currentLine) {
        if (currentLine.equals("random")) {
            return;
        }

        if (topicStringsToInts == null) {
            topicStringsToInts = new HashMap<>();
        }
        if (nodesToGroups == null) {
            nodesToGroups = new HashMap<>();
        }

        String[] lineContent = currentLine.split(" ");


        long nodeId = Long.valueOf(lineContent[0]);

        for (int groupIdx = 1; groupIdx < lineContent.length; groupIdx++) {
            String groupName = lineContent[groupIdx];
            int groupId = topicStringsToInts
                    .computeIfAbsent(groupName, k -> topicStringsToInts.size());

            nodesToGroups.computeIfAbsent(nodeId, k -> new HashSet<>())
                    .add(groupId);
        }
    }

    private void handleActionsInfo(String currentLine) {
        if (currentLine.equals("random")) {
            return;
        }

        if (actions == null) {
            actions = new HashMap<>();
        }

        String[] lineContent = currentLine.split(" ");

        Long time = Long.valueOf(lineContent[0]);

        String action =
                lineContent[1] + " " + // nodeId
                lineContent[2]; // topic
        actions.computeIfAbsent(time, k -> new ArrayList<>()).add(action);
    }

    public List<Pair<Integer, Integer>> getConnections() {
        return connections;
    }

    public Map<Long, Set<Integer>> getGroupsInfo() {
        return nodesToGroups;
    }

    public boolean isInFileMode() {
        return actions != null;
    }

    /**
     * A list of actions can only be retrieved once, in order to ensure
     * each action only happens once.
     * @param time
     * @return
     */
    public List<String> getActionsForTime(long time) {
        List<String> result = actions.get(time);
        actions.remove(time);
        return result;
    }

    public String getTopologyType() {
        return topologyType;
    }

    private String updateNextBufReaderLine(BufferedReader bufferedReader) {
        String result;
        try {
            result = bufferedReader.readLine();
            if (result == null) {
                throw new ItsOverException();
            }
            while (result.isEmpty() || result.startsWith("#")) {
                result = bufferedReader.readLine();
                if (result == null) {
                    throw new ItsOverException();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Random error reading a line");
        }
        System.out.println("NewLine: " + result);
        return result;
    }

    private class ItsOverException extends RuntimeException {

    }
}
