package example.occult.datatypes;

import example.common.datatypes.Operation;

public class ReadOperation extends Operation {

	private boolean migrateToMaster;

	public ReadOperation(int key, boolean migrateToMaster) {
		super(Type.READ, key);
		this.migrateToMaster = migrateToMaster;
	}

	public void setMigration(boolean bool) {
		migrateToMaster = bool;
	}

	public boolean migrateToMaster() {
		return migrateToMaster;
	}

}
