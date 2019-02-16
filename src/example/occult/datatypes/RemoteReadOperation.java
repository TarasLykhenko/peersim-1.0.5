package example.occult.datatypes;

public class RemoteReadOperation extends Operation {

	public RemoteReadOperation(int key) {
		super(Type.REMOTE_READ, key);
	}

}
