package base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backThread.DataPersistenceThread;
import backThread.QueryThread;
import util.XML;

class Callback extends OutPutCallBack {
	public void out(List<Map<String, Object>> r) {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for (int i = 0; i < r.size(); ++i) {
			for(String key : r.get(i).keySet()) {
				System.out.print(key+":"+r.get(i).get(key)+" ");
			}
			System.out.println();
		}
		System.out.println();
	}
}

class thread1 implements Runnable {
	public void run(){
		WCache wc = WCache.getInstance();
		Callback opcb = new Callback();
		Table test = wc.tables.get("test");
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("b", "22b");
		Trasaction t = new Trasaction();// 事务测试
		test.query().update(params).where("a").eq("2a").addToTrasaction(t, null);
		
		test.query().select().where("a").eq("2a").addToTrasaction(t, opcb);
		t.trasactionExecute();
	}
}

class thread2 implements Runnable {
	public void run(){
		
		WCache wc = WCache.getInstance();
		Callback opcb = new Callback();
		Table test = wc.tables.get("test");
		
		Trasaction t = new Trasaction();// 事务测试
		test.query().select().where("a").eq("2a").addToTrasaction(t, opcb);
		t.trasactionExecute();
	}
}

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
	
	// 事务测试
	public static void main(String[] args) {
		new Thread(new thread2()).start();
		new Thread(new thread1()).start();
	}
}
