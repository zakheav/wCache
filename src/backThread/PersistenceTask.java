package backThread;

import base.MySQL;

public class PersistenceTask implements Runnable{
	private String SQL = "";
	public PersistenceTask(String sql) {
		SQL = sql;
	}
	
	public void run() {
		MySQL.getInstance().executeUpdate(SQL);
	}
}
