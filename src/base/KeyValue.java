package base;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KeyValue {
	public Map<Object, Set<Row>> kv = new HashMap<Object, Set<Row>>();// 每个value对应table中的一行的引用
}
