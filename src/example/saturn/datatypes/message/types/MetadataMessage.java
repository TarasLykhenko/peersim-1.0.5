package example.saturn.datatypes.message.types;

public class MetadataMessage extends Message{

	long updateID;

	public MetadataMessage(long updateID) {
		super(Type.METADATA, 0,ChannelType.TCP);
		this.updateID = updateID;
		this.setMessageSize(calculateMessageSize());

	}

	public long getUpdateID(){
		return updateID;
	}

	protected int calculateMessageSize(){
		int size = super.calculateMessageSize();
		size += 8; //updateID
		return size;
	}
}
