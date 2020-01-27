package example.saturn.components;

import javafx.util.Pair;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TreeHelper {

    Node dc;
    Random rand = new Random();
    static AtomicLong ID = new AtomicLong(0);

    public void createTree(int depth, Pair<Integer,Integer> range){

        dc = new Node(ID.getAndIncrement(),null);
        createTreeHelper(dc, depth, range);
    }

    //range <min, max>
    public void createTreeHelper(Node parent, int depth, Pair<Integer,Integer> range){

        if (depth <= 0) return;

        depth--;
        int leafsRange = rand.nextInt((range.getValue() - range.getKey()) + 1) + range.getKey();

        for (int i = 0; i < leafsRange; i++ ){
            Node newNode = new Node(ID.getAndIncrement(), parent);
            parent.addChild(newNode);
            createTreeHelper(newNode, depth, range);
        }

    }

    public Long getTreeSize(){
        return ID.get();
    }
}
