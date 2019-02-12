package example.ctreeplus;

import java.util.HashMap;

public class Matrix {
	
	HashMap<Integer, HashMap<Integer, Integer>> matrix;

	
	public Matrix(long nodeid, int timestamp){
		matrix = new HashMap<Integer, HashMap<Integer, Integer>>();
	}
	
	public int get(int x, int y){
		if (matrix.containsKey(x)){
			HashMap<Integer, Integer> array = matrix.get(x);
			if (array.containsKey(y)){
				return array.get(y);
			}else{
				return -1;
			}
		}else{
			return -1;
		}
	}
	
	public void put(int counter, int i, int value){
		if (matrix.containsKey(counter)){
			HashMap<Integer, Integer> array = matrix.get(counter);
			array.put(i, value);
		}else{
			HashMap<Integer, Integer> array = new HashMap<Integer, Integer>();
			array.put(i, value);
			matrix.put(counter, array);
		}
	}
}
