package example.cops.datatypes;

public class Operation {

	public enum Type {
		READ,
		REMOTE_READ,
		UPDATE,
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
