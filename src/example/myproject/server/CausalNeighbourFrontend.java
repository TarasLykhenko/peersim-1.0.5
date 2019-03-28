package example.myproject.server;

import example.myproject.Initialization;
import example.myproject.Sizeable;
import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.PathMessage;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CausalNeighbourFrontend extends CausalNeighbourBackend
        implements CDProtocol, EDProtocol {

    private final int dcPid;
    private final int linkablePid;
    private final String prefix;
    private final int publisher; // TODO isto e uma temp var

    private static final String PAR_EXECUTION_MODEL = "execution-model";
    private static final String PAR_EXECUTION_MODEL_FILENAME = "execution-model-file";
    private static final BufferedReader executionFile;
    private static String bufReaderCurrentLine = "";
    private static final String executionModel;

    static {
        try {
            executionModel = Configuration.getString(PAR_EXECUTION_MODEL);
            String fileName = Configuration.getString(PAR_EXECUTION_MODEL_FILENAME);
            executionFile = new BufferedReader(new FileReader("example/other/" + fileName));
            updateNextBufReaderLine();
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Could not get execution model file");
        }
    }

    public CausalNeighbourFrontend(String prefix) {
        super(prefix);
        this.prefix = prefix;
        this.dcPid = Configuration.getPid("causalneighbour");
        this.linkablePid = Configuration.getPid("linkable");
        this.publisher = Configuration.getInt("publisher");
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
        if (isCrashed()) {
            return;
        }

        checkCrashedConnections();

        if (executionModel.equals("file")) {
            CausalNeighbourFrontend.staticNextCycleFile(protocolID);
        } else if (executionModel.equals("random")) {
            nextCycleRandom(node, protocolID);
        } else {
            throw new AssertException("Unknown execution model");
        }
    }

    private static void staticNextCycleFile(int protocolID) {
        if (bufReaderCurrentLine == null) {
            return;
        }

        while (true) {
            String[] lineContent = bufReaderCurrentLine.split(" ");
            long currentTime = CommonState.getTime();
            long obtainedTime = Long.valueOf(lineContent[0]);

            if (currentTime != obtainedTime) {
                break;
            }
            System.out.println("CURRENT TIME!: " + currentTime);

            int nodeId = Integer.valueOf(lineContent[1]);
            String action = lineContent[2];
            Node node = Network.get(nodeId);
            CausalNeighbourFrontend frontend = (CausalNeighbourFrontend) node.getProtocol(protocolID);

            if (action.equals("crash")) {
                frontend.crash();
            } else if (action.equals("uncrash")) {
                frontend.unCrash();
            } else {
                int topic = Initialization.stringGroupToInteger.get(action);
                List<Message> newMessages = frontend.publishMessage(topic);
                frontend.frontendForwardMessage(newMessages, protocolID);
            }

            updateNextBufReaderLine();
            if (bufReaderCurrentLine == null) {
                return;
            }
        }
    }

    private static void updateNextBufReaderLine() {
        try {
            bufReaderCurrentLine = executionFile.readLine();
            if (bufReaderCurrentLine == null) {
                return;
            }
            while (bufReaderCurrentLine.isEmpty() || bufReaderCurrentLine.startsWith("#")) {
                bufReaderCurrentLine = executionFile.readLine();
                if (bufReaderCurrentLine == null) {
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertException("Random error reading a line");
        }
        System.out.println("NewLine: " + bufReaderCurrentLine);
    }

    private void nextCycleRandom(Node node, int protocolID) {
        //TODO TIRAR ISTO NÉ
        if (node.getID() != publisher) {
            return;
        }
        // System.out.println("PUBLISHER: " + node.getID());

        List<Message> newMessages = publishMessage();

        frontendForwardMessage(newMessages, protocolID);
    }

    @Override
    public void unCrash() {
        if (!isCrashed) {
            throw new AssertException("Reviving an alive server.");
        }

        this.isCrashed = false;

        Node thisNode = Network.get((int) id);
        System.out.println("Crash over at " + id + ". Processing messages.");
        for (Message message : queuedMessagesReceived) {
            processEvent(thisNode, dcPid, message);
        }
        queuedMessagesReceived.clear();
    }

    /**
     * NOTE: This message doesn't take any time sending events, as it's all
     * in-process calls.
     *
     * @param pathMessage
     */
    @Override
    public void receivePath(PathMessage pathMessage) {
        boolean shouldSpread = processNewReceivedPath(pathMessage);

        if (shouldSpread) {
            Node thisNode = Network.get(Math.toIntExact(id));
            Linkable linkable = (Linkable) thisNode.getProtocol(linkablePid);

            PathMessage newPathMessage = new PathMessage(pathMessage, id);
            for (int i = 0; i < linkable.degree(); i++) {
                Node neighbor = linkable.getNeighbor(i);

                // Don't send back the message
                if (neighbor.getID() == pathMessage.getSender()) {
                    continue;
                }

                System.out.println("NODE " + id + " SENDING PATH " + newPathMessage.getNodePath() + " TO " + neighbor.getID());
                Utils.nodeToBackend(neighbor).receivePath(newPathMessage);
            }
        }
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
            System.out.println("(Node " + id + ") Received startConnection request from " + msg.sender);
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
        System.out.println("(Node " + id + ") sending a connection request to " + target);
        sendMessage(getId(), target, msg, dcPid);
    }

    private void frontendForwardMessage(List<Message> messages, int pid) {
        if (messages.isEmpty()) {
            return;
        }

        System.out.println("---- Node " + id + " forwarding messages (" + messages.size() + ") ---");
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

    class StartConnectionMsg implements Sizeable {

        final long sender;
        final long target;

        StartConnectionMsg(long sender, long target) {
            this.sender = sender;
            this.target = target;
        }

        /**
         * Size is 2 longs, each long is 8 bytes, therefore 16.
         */
        @Override
        public long getSize() {
            return 16;
        }
    }

    class MessageHistoryMsg implements Sizeable {

        final long sender;
        final long target;
        final List<Long> historyFromSender;

        MessageHistoryMsg(long sender, long target, List<Long> historyFromSender) {
            this.sender = sender;
            this.target = target;
            this.historyFromSender = new ArrayList<>(historyFromSender);
        }

        /**
         * Size is 2 longs (2*8) + (size of History * 8)each long is 8 bytes
         */
        @Override
        public long getSize() {
            return 16 + (historyFromSender.size() * 8);
        }
    }
}