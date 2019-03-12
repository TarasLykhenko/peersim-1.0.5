package example.myproject.datatypes;

import peersim.core.Node;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NodePath {

    public List<Node> path;
    public Set<Node> fullPathSet;
    public Set<Node> pathSetWithoutStart;
    private String pathString;

    public NodePath(List<Node> path) {
        this.path = Collections.unmodifiableList(path);
        this.fullPathSet = Collections.unmodifiableSet(new HashSet<>(path));
        Set<Node> setWithoutStart = new HashSet<>(path);
        setWithoutStart.remove(path.get(0));
        this.pathSetWithoutStart = Collections.unmodifiableSet(setWithoutStart);
        this.pathString = path.stream()
                .map(Node::getID)
                .map(Object::toString)
                .collect(Collectors.joining(":"));
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
}
