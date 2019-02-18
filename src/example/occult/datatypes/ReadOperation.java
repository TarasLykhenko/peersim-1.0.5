package example.occult.datatypes;

public class ReadOperation extends Operation {

	private boolean migrateToMaster;

	public ReadOperation(int key, boolean migrateToMaster) {
		super(Type.READ, key);
		this.migrateToMaster = migrateToMaster;
		// TODO Auto-generated constructor stub
	}

	public void setMigration(boolean bool) {
		migrateToMaster = bool;
	}

	public boolean migrateToMaster() {
		return migrateToMaster;
	}

}
