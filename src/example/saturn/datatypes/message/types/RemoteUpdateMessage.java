package example.saturn.datatypes.message.types;

import javafx.util.Pair;

public class RemoteUpdateMessage extends Message{

	long updateID;
	long value; //key value

	public RemoteUpdateMessage(int key, long _value, long _updateID) {
		super(Type.REMOTEUPDATE, key, ChannelType.UDP);
		value = _value;
		updateID = _updateID;
	}

	public long getUpdateID(){
		return updateID;
	}

	public long getValue(){
		return value;
	}


}
