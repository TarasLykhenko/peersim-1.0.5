package example.saturn;

import java.util.LinkedList;
import java.util.Queue;

public class Broker {

    Queue<Long> metaQueue = new LinkedList<>();

    public void newUpdate(Long newUpdateId) {
        metaQueue.add(newUpdateId);
    }

}
