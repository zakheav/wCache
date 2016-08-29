package base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Table {// 一个缓存对象
	
	public String name;

	public Query query() {
		return new Query(this);
	};

	private ArrayList<String> updateQueryString = new ArrayList<String>();// 用于存储需要修改持久化层的sql语句
	// 为了优化where语句而添加的table拆分后的缓存DivideTableCache
	private Map<String, KeyValue> dtc = new HashMap<String, KeyValue>();// dtc的key表示其对应的value（KeyValue）对象中的key是table中的哪一列
	private Set<Row> table = new HashSet<Row>();// 存储缓存
	public Map<String, KeyValue> getDTC() {
		return dtc;
	}
	public Set<Row> getTable() {
		return table;
	}
	public ArrayList<String> getUpadateQueryString() {
		return updateQueryString;
	}
	
	public List<Map<String, Object>> out() {
		int count = 0;
		List<Map<String, Object>> outPut = new ArrayList<Map<String, Object>>();
		for (Row row : table) {
			Map<String, Object> r = row.read();
			outPut.add(r);
			if (count == 0) {
				for (String key : r.keySet()) {
					System.out.printf("%-10s", key + "");
				}
				System.out.println();
				System.out.println("----------------------");
			}
			for (String key : r.keySet()) {
				System.out.printf("%-10s", r.get(key) + "");
			}
			System.out.println();
			++count;
		}
		System.out.println("total: " + count);
		return outPut;
	}// 输出所有内容

	public Table(String n) {
		name = n;
		// 生成name指定的mysql表在内存中的镜像table
		String sql = "select * from " + name + " where 1";
		List<Map<String, Object>> temp = MySQL.getInstance().executeQuery(sql);
		// 生成table
		for (int i = 0; i < temp.size(); ++i) {
			table.add(new Row(temp.get(i), this));
		}
	}
}
