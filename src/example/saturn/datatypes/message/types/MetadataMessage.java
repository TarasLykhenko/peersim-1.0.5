package example.saturn.datatypes.message.types;

public class MetadataMessage extends Message{

	long updateID;

	public MetadataMessage(long updateID) {
		super(Type.METADATA, 0);
		this.updateID = updateID;
	}

	public long getUpdateID(){
		return updateID;
	}
}
