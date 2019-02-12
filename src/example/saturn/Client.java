package example.saturn;

import example.saturn.datatypes.Operation;
import example.saturn.datatypes.ReadOperation;
import example.saturn.datatypes.UpdateOperation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class Client {
    /*
     * OPERATIONS PROBABILITIES BASED ON F. Benevenuto et al. (IMC'09)
     * id	description							prob		norm. prob		random number
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * 1:	Universal search					02.0%		277				277
     * 4: 	Browse messages						00.5%		69				346
     * 5: 	Write messages						00.1%		14				360
     * 11:	Browse a list of albums				05.6%		775				1135
     * 12:	Browse photo albums					08.9%		1231			2366
     * 13:	Browse photos						15.5%		2144			4510
     * 14:	Browse photos the user was tagged	00.4%		55				4565
     * 15:	Browse photo comment				00.1%		14				4579
     * 16:	Edit and organize photos			00.0%		0				4579
     * 17:	Browse profiles						19.0%		2628			7207
     * 18:	Browse homepage						11.8%		1632			8839
     * 19:	Browse the list of friends			06.4%		885				9724
     * 22:	Browse friend updates				00.8%		111				9835
     * 24:	Profile editing						00.9%		124				9959
     * 39:	User settings						00.3%		41				10000
     * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     * TOTAL:									72.3%		100%
     *
     * writes %												179  = 	1.79%	 179
     *
     * myself writes %										 14	 =  0.14%	  14
     * friends writes %										165  =  1.65%	 179
     *
     * reads %												9821 = 98.21%  10000
     *
     * myself read %										1756 = 17.56%	1756
     * friend or myself read %								5049 = 50.49%	6805
     * friend read %										2739 = 27.39%	9544
     * anyone read %										 277 =  2.77%	9821
     */

    public int getId() {
        return id;
    }

    private int id;
    private int totalClients;
    private int previousOperation;
    private int friendsSize;
    private int counter;
    private int keysPerNode;
    private List<Integer> friends;
    private Set<String> allKeys;
    private Set<String> trimKeys;
    private Random randomGenerator = new Random();


    public Client(int id, int total, int keysPerNode, Set<Integer> friends) {
        super();
        this.id = id;
        this.totalClients = total;
        this.previousOperation = -1;
        this.friends = new ArrayList<>();
        this.friends.addAll(friends);
        this.friendsSize = friends.size();
        this.counter = 0;
        this.allKeys = new HashSet<>();
        this.trimKeys = new HashSet<>();
        this.keysPerNode = keysPerNode;
    }

    private static boolean isBetween(int x, int lower, int upper) {
        return lower <= x && x <= upper;
    }

    public int timestamp() {
        return counter++;
    }

    public Set<String> getAllKeys() {
        return allKeys;
    }

    public void setAllKeys(Set<String> allKeys) {
        this.allKeys = allKeys;
    }

    public Set<String> getTrimKeys() {
        return trimKeys;
    }

    public void setTrimKeys(Set<String> trimKeys) {
        this.trimKeys = trimKeys;
    }

    public List<Operation> nextOperationTest() {
        List<Operation> operations = new ArrayList<>();
        int num = randomGenerator.nextInt(100);
        if (isBetween(num, 0, 90)) {
            int key = id;
            addReadOperation(operations, randomGenerator, key);
        } else if (isBetween(num, 91, 100)) {
            addUpdateOperation(operations, randomGenerator, id);
        } else {
            System.out.println("ERROR generating operation for client: " + id + ". Random value: " + num);
            //return -1;
        }
        return operations;
    }

    public List<Operation> nextOperation() {
        ArrayList<Operation> operations = new ArrayList<>();
        int num = randomGenerator.nextInt(10000);
        if (isBetween(num, 0, 277)) {
            //  1:	Universal search					02.0%		277
            int key = randomGenerator.nextInt(totalClients);
            addReadOperation(operations, randomGenerator, key);

        } else if (isBetween(num, 278, 346)) {
            // 4: 	Browse messages						00.5%		346
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 347, 360)) {
            // 5: 	Write messages						00.1%		360
            int index = randomGenerator.nextInt(friendsSize);
            int key = friends.get(index);
            addUpdateOperation(operations, randomGenerator, id);
            addUpdateOperation(operations, randomGenerator, key);

        } else if (isBetween(num, 361, 1135)) {
            // 11:	Browse a list of albums				05.6%		1135
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 1136, 2366)) {
            // 12:	Browse photo albums					08.9%		2366
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 2367, 4510)) {
            // 13:	Browse photos						15.5%		4510
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 4511, 4565)) {
            // 14:	Browse photos the user was tagged	00.4%		4565
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 4566, 4579)) {
            // 15:	Browse photo comment				00.1%		4579
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 4580, 7207)) {
            // 17:	Browse profiles						19.0%		7207
            int index = randomGenerator.nextInt(friendsSize);
            int key = friends.get(index);
            addReadOperation(operations, randomGenerator, key);

        } else if (isBetween(num, 7208, 8839)) {
            // 18:	Browse homepage						11.8%		8839
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 8840, 9724)) {
            // 19:	Browse the list of friends			06.4%		9724
            addReadOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 9725, 9835)) {
            // 22:	Browse friend updates				00.8%		9835
            int index = randomGenerator.nextInt(friendsSize);
            int key = friends.get(index);
            addReadOperation(operations, randomGenerator, key);

        } else if (isBetween(num, 9836, 9959)) {
            // 24:	Profile editing						00.9%		9959
            addUpdateOperation(operations, randomGenerator, id);

        } else if (isBetween(num, 9960, 10000)) {
            // 39:	User settings						00.3%		10000
            addUpdateOperation(operations, randomGenerator, id);

        } else {
            System.out.println("ERROR generating operation for client: " + id + ". Random value: " + num);
        }
        return operations;
    }

    private void addUpdateOperation(List<Operation> operations, Random randomGenerator, int id) {
        int intraKey1 = randomGenerator.nextInt(keysPerNode);
        String keyMetadata1 = id + "," + intraKey1;
        operations.add(new UpdateOperation(id, trimKeys.size(), allKeys.size(), keyMetadata1));
        allKeys.add(keyMetadata1);
        trimKeys.clear();
        trimKeys.add(keyMetadata1);
    }

    private void addReadOperation(List<Operation> operations, Random randomGenerator, int key) {
        operations.add(new ReadOperation(key));
        int intraKey = randomGenerator.nextInt(keysPerNode);
        String keyMetadata = key + "," + intraKey;
        allKeys.add(keyMetadata);
        trimKeys.add(keyMetadata);
    }

    public Client clone() {
        TreeSet<Integer> copyFriends = new TreeSet<>(friends);
        Client newClient = new Client(id, totalClients, keysPerNode, copyFriends);
        HashSet<String> all = new HashSet<>(allKeys);
        HashSet<String> trim = new HashSet<>(trimKeys);
        newClient.setAllKeys(all);
        newClient.setTrimKeys(trim);
        return newClient;
    }
}
