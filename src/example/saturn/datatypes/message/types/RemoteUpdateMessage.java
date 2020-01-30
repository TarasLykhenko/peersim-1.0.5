package example.saturn.datatypes.message.types;

import javafx.util.Pair;

import static example.common.Settings.VALUE_SIZE;

public class RemoteUpdateMessage extends Message{

	long updateID;
	long value; //key value

	public RemoteUpdateMessage(int key, long _value, long _updateID) {
		super(Type.REMOTEUPDATE, key, ChannelType.UDP);
		value = _value;
		updateID = _updateID;
		this.setMessageSize(calculateMessageSize());
	}

	public long getUpdateID(){
		return updateID;
	}

	public long getValue(){
		return value;
	}

	protected int calculateMessageSize(){
		int size = super.calculateMessageSize();
		size += VALUE_SIZE;
		size += 8;
		return size;
	}


}
