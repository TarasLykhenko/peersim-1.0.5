package example.myproject.server;

import example.myproject.Initialization;
import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CausalNeighbourFrontend extends CausalNeighbourBackend
        implements CDProtocol, EDProtocol {

    private final int dcPid;
    private final String prefix;
    private final int publisher; // TODO isto e uma temp var

    private final String PAR_EXECUTION_MODEL = "execution-model";
    private final String PAR_EXECUTION_MODEL_FILENAME = "execution-model-file";
    private final String executionModel;
    private final BufferedReader executionFile;
    private String bufReaderCurrentLine = "";

    public CausalNeighbourFrontend(String prefix) {
        super(prefix);
        this.prefix = prefix;
        this.dcPid = Configuration.getPid("causalneighbour");
        this.publisher = Configuration.getInt("publisher");
        executionModel = Configuration.getString(PAR_EXECUTION_MODEL);
        String fileName = Configuration.getString(PAR_EXECUTION_MODEL_FILENAME);

        try {
            executionFile = new BufferedReader(new FileReader("example/other/" + fileName));
            bufReaderCurrentLine = executionFile.readLine();
            while (bufReaderCurrentLine.startsWith("#")) {
                bufReaderCurrentLine = executionFile.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Could not get execution model file");
        }
    }


    /**
     * Algorithm:
     *  1) Generate message
     *  2) See which direct neighbours are interested in the message
     *      2.1) If the direct neighbours are down,
     *          check their directory neighbours (and so on until delta)
     *
     *  3) Check the nodes that are on the same path (so that they have the same
     *      metadata
     *  4) Add metadata to each message to be sent
     *  5) Send the message to each path.
     *
     */
    @Override
    public void nextCycle(Node node, int protocolID) {
        if (executionModel.equals("file")) {
            nextCycleFile(node, protocolID);
        } else if (executionModel.equals("random")) {
            nextCycleRandom(node, protocolID);
        } else {
            throw new AssertException("Unknown execution model");
        }
    }

    private void nextCycleFile(Node node, int protocolID) {
        if (bufReaderCurrentLine == null) {
            return;
        }
        // Isto tem um bocadinho de duplicate code, mas whatever
        String[] lineContent = bufReaderCurrentLine.split(" ");
        long currentTime = CommonState.getTime();
        long obtainedTime = Long.valueOf(lineContent[0]);
        int nodeId = Integer.valueOf(lineContent[1]);
        String action = lineContent[2];

        while (currentTime != obtainedTime) {
            if (node.getID() != nodeId) {
                updateNextBufReaderLine();
                System.out.println("Current line: " + bufReaderCurrentLine);
                if (bufReaderCurrentLine == null) {
                    return;
                }

                lineContent = bufReaderCurrentLine.split(" ");
                obtainedTime = Long.valueOf(lineContent[0]);
                nodeId = Integer.valueOf(lineContent[1]);
                action = lineContent[2];
                continue;
            }

            if (action.equals("crash")) {
                crash();
            } else if (action.equals("uncrash")) {
                unCrash();
            } else {
                int topic = Initialization.stringGroupToInteger.get(action);
                List<Message> newMessages = publishMessage(topic);
                frontendForwardMessage(newMessages, protocolID);
            }

            updateNextBufReaderLine();

            if (bufReaderCurrentLine == null) {
                return;
            }

            lineContent = bufReaderCurrentLine.split(" ");
            obtainedTime = Long.valueOf(lineContent[0]);
            nodeId = Integer.valueOf(lineContent[1]);
            action = lineContent[2];
        }
    }

    private static void staticNextCycleFile() {
        if (bufReaderCurrentLine == null) {
            return;
        }
        // Isto tem um bocadinho de duplicate code, mas whatever
        String[] lineContent = bufReaderCurrentLine.split(" ");
        long currentTime = CommonState.getTime();
        long obtainedTime = Long.valueOf(lineContent[0]);
        int nodeId = Integer.valueOf(lineContent[1]);
        String action = lineContent[2];

    }

    private void updateNextBufReaderLine() {
        try {
            bufReaderCurrentLine = executionFile.readLine();
            if (bufReaderCurrentLine == null) {
                return;
            }
            while (bufReaderCurrentLine.isEmpty()) {
                bufReaderCurrentLine = executionFile.readLine();
                if (bufReaderCurrentLine == null) {
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Random error reading a line");
        }
    }

    private void nextCycleRandom(Node node, int protocolID) {
        if (isCrashed()) {
            return;
        }

        checkCrashedConnections();

        //TODO TIRAR ISTO NÉ
        if (node.getID() != publisher) {
            return;
        }
        // System.out.println("PUBLISHER: " + node.getID());

        List<Message> newMessages = publishMessage();

        frontendForwardMessage(newMessages, protocolID);
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
        if (event instanceof Message) {
            Message message = (Message) event;
            System.out.println("---------------------");
            System.out.println("Received message: (Node " + node.getID() + ")");
            message.printMessage();
            List<Message> messagesToForward = this.receiveMessage(message);
            frontendForwardMessage(messagesToForward, pid);
        } else if (event instanceof StartConnectionMsg) {
            StartConnectionMsg msg = (StartConnectionMsg) event;
            System.out.println("Received startConnection request");
            List<Long> messageHistory = this.handleNewConnection(msg.sender);
            MessageHistoryMsg messageHistoryMsg = new MessageHistoryMsg(this.getId(), msg.sender, messageHistory);
            sendMessage(this.getId(), msg.sender, messageHistoryMsg, pid);
        } else if (event instanceof MessageHistoryMsg) {
            MessageHistoryMsg msg = (MessageHistoryMsg) event;
            List<Message> missingMessages = this.compareHistory(msg.sender, msg.historyFromSender);
            frontendForwardMessage(missingMessages, pid);
        } else {
            throw new AssertException("Unknown message type. " + event.getClass().getSimpleName());
        }
    }

    @Override
    public void startConnection(long target) {
        StartConnectionMsg msg = new StartConnectionMsg(getId(), target);
        sendMessage(getId(), target, msg, dcPid);
    }

    private void frontendForwardMessage(List<Message> messages, int pid) {
        if (messages.isEmpty()) {
            return;
        }

        System.out.println("---- Forwarding messages (" + messages.size() + ") ---");
        for (Message messageToForward : messages) {
            messageToForward.printMessage();
            frontendForwardMessage(messageToForward, pid);
        }
        System.out.println("---------------------------");
        System.out.println();
    }

    private void frontendForwardMessage(Message message, int pid) {
        Node srcNode = Network.get(Math.toIntExact(this.getId()));
        Node targetNode = Network.get(Math.toIntExact(message.getNextDestination()));

        if (Utils.nodeToBackend(targetNode).isCrashed()) {
            System.out.println("Wasting time..");
        }

        sendMessage(srcNode, targetNode, message, pid);
    }

    private void sendMessage(long src, long dst, Object msg, int pid) {
        Node srcNode = Network.get(Math.toIntExact(src));
        Node targetNode = Network.get(Math.toIntExact(dst));

        sendMessage(srcNode, targetNode, msg, pid);
    }

    private void sendMessage(Node src, Node dst, Object msg, int pid) {
        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

    @Override
    public Object clone() {
        return new CausalNeighbourFrontend(prefix);
    }

    class StartConnectionMsg {

        final long sender;
        final long target;

        StartConnectionMsg(long sender, long target) {
            this.sender = sender;
            this.target = target;
        }
    }

    class MessageHistoryMsg {

        final long sender;
        final long target;
        final List<Long> historyFromSender;

        MessageHistoryMsg(long sender, long target, List<Long> historyFromSender) {
            this.sender = sender;
            this.target = target;
            this.historyFromSender = new ArrayList<>(historyFromSender);
        }
    }
}