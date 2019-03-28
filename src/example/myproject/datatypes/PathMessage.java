package example.myproject.datatypes;

public class PathMessage {

    private final long pathId;
    private final NodePath nodePath;
    private final int currentDistance;
    private final long sender;

    public PathMessage(long pathId, NodePath nodePath, int currentDistance, long sender) {
        this.pathId = pathId;
        this.nodePath = nodePath;
        this.currentDistance = currentDistance;
        this.sender = sender;
    }

    public PathMessage(PathMessage pathMessage, long sender) {
        this.pathId = pathMessage.pathId;
        this.nodePath = pathMessage.nodePath;
        this.currentDistance = pathMessage.currentDistance + 1;
        this.sender = sender;
    }

    public long getPathId() {
        return pathId;
    }

    public NodePath getNodePath() {
        return nodePath;
    }

    public int getCurrentDistance() {
        return currentDistance;
    }

    public long getSender() {
        return sender;
    }
}
