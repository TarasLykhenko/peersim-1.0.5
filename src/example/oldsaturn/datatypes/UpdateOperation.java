package example.oldsaturn.datatypes;

public class UpdateOperation extends Operation{

	int metadataFull;
	int metadataPartial;
	String keyMetadata;
	
	public UpdateOperation(int key, int full, int partial, String metaKey) {
		super(1, key);
		metadataFull = full;
		metadataPartial = partial;
		keyMetadata = metaKey;
	}

	public int getMetadataFull() {
		return metadataFull;
	}

	public void setMetadataFull(int metadataFull) {
		this.metadataFull = metadataFull;
	}

	public int getMetadataPartial() {
		return metadataPartial;
	}

	public void setMetadataPartial(int metadataPartial) {
		this.metadataPartial = metadataPartial;
	}

	public String getKeyMetadata() {
		return keyMetadata;
	}

	public void setKeyMetadata(String keyMetadata) {
		this.keyMetadata = keyMetadata;
	}

	
}
