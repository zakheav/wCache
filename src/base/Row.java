package base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Row {
	private Table table;
	public Map<String, Object> row;
	public Row(Map<String, Object> r, Table t) {
		row = r;
		table = t;
	}

	public boolean write(Map<String, Object> keyValue) {
		// 删除table.dtc中的数据
		for (String key : keyValue.keySet()) {
			if (table.getDTC().containsKey(key)) {
				Map<Object, Set<Row>> kv = table.getDTC().get(key).kv;
				Object oldValue = row.get(key);
				kv.get(oldValue).remove(this);// 删除旧的值
				if (kv.get(oldValue).isEmpty()) {
					kv.remove(oldValue);
				}
			}
		}
		// 完成写操作
		for (String key : keyValue.keySet()) {
			if (!row.containsKey(key)) {
				System.out.println("error: no such column '" + key + "' in table: " + table.name);
				return false;
			} else {
				row.put(key, keyValue.get(key));
			}
		}
		// 向table.dtc中增加数据
		for (String key : keyValue.keySet()) {
			if (table.getDTC().containsKey(key)) {
				Map<Object, Set<Row>> kv = table.getDTC().get(key).kv;
				Object value = keyValue.get(key);
				if (!kv.containsKey(value)) {
					kv.put(value, new HashSet<Row>());
				}
				kv.get(value).add(this);
			}
		}
		return true;
	}

	public Map<String, Object> read() {
		Map<String, Object> r = new HashMap<String, Object>();
		for (String key : row.keySet()) {
			r.put(key, row.get(key));
		}
		return r;
	}
}