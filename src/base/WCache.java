package base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backThread.DataPersistenceThread;
import backThread.QueryThread;
import util.XML;

public class WCache {
	private static WCache instance = new WCache();
	public static WCache getInstance() {
		return instance;
	}
	public Map<String, Table> tables = new HashMap<String, Table>();
	private WCache() {
		// 启动后台线程
		QueryThread.getInstance();
		DataPersistenceThread.getInstance();
		// 获得需要增加缓存的表
		List<String> tablesName = new XML().tableConf();
		for(int i=0; i<tablesName.size(); ++i) {
			String name = tablesName.get(i);
			tables.put(name, new Table(name));
		}
	}
}
