package example.genericsaturn.datatypes;

public class Operation {
	/* 0:	read
	 * 1: 	update
	 * 2:	remote read
	 */
	public
	enum Type {
		READ,
		UPDATE,
		REMOTE_READ
	}

	private Type type;
	private int key;

	public Operation(Type type, int key) {
		super();
		this.type = type;
		this.key = key;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}
}
