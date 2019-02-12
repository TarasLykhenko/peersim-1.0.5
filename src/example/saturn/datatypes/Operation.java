package example.saturn.datatypes;

public class Operation {
	/* 0:	read
	 * 1: 	update
	 * 2:	remote read
	 */
	int Type;
	int key;
	
	public Operation(int type, int key) {
		super();
		Type = type;
		this.key = key;
	}

	public int getType() {
		return Type;
	}

	public void setType(int type) {
		Type = type;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}
}
