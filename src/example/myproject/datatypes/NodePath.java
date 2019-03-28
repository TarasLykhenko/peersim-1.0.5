package example.myproject.datatypes;

import example.myproject.Utils;
import peersim.core.Node;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NodePath {

    public final long id;
    public final List<Node> path;
    public final Set<Node> fullPathSet;
    public final Set<Long> fullPathSetLongs;
    public final Set<Node> pathSetWithoutStart;
    private final String pathString;

    public NodePath(List<Node> path, long id) {
        this.id = id;
        this.path = Collections.unmodifiableList(path);
        this.fullPathSet = Collections.unmodifiableSet(new HashSet<>(path));
        Set<Node> setWithoutStart = new HashSet<>(path);
        setWithoutStart.remove(path.get(0));
        this.pathSetWithoutStart = Collections.unmodifiableSet(setWithoutStart);
        this.pathString = path.stream()
                .map(Node::getID)
                .map(Object::toString)
                .collect(Collectors.joining(":"));
        this.fullPathSetLongs = path.stream().map(Node::getID).collect(Collectors.toSet());
    }

    public Node getLastNodeOnPath() {
        return Utils.getLastEntry(path);
    }

    public void printLn(String msg) {
        String result = path.stream()
                .map(Node::getID)
                .map(Object::toString)
                .collect(Collectors.joining(":"));
        System.out.println(msg + " - " + result);
    }

    public String getPathString() {
        return pathString;
    }

    @Override
    public String toString() {
        return pathString;
    }

    public static void printPathLongs(String msg, Collection<Long> path) {
        String result = path.stream()
                .map(Object::toString)
                .collect(Collectors.joining(":"));
        System.out.println(msg + " - " + result);
    }

    public boolean containsNode(long id) {
        return fullPathSetLongs.contains(id);
    }
}
