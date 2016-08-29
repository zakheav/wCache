package backThread;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import base.KeyValue;
import base.OutPutCallBack;
import base.Query;
import base.Row;
import base.Table;

public class QueryTask implements Runnable {
	private Query query;
	private OutPutCallBack out;
	private Stack<String> ops = new Stack<String>();// 操作符栈
	private Stack<Set<Row>> ds = new Stack<Set<Row>>();// 操作数栈
	// 逻辑连接符优先级表 -1优先级低 1优先级高 2结束
	/*
	 * ops tokens and or # and -1 1 1 or -1 -1 1 # -1 -1 2
	 */
	private int[][] priorityTable = new int[3][3];

	public QueryTask(Query q, OutPutCallBack o) {
		query = q;
		out = o;
		ops.add("#");
		priorityTable[0][0] = -1;
		priorityTable[0][1] = 1;
		priorityTable[0][2] = 1;
		priorityTable[1][0] = -1;
		priorityTable[1][1] = -1;
		priorityTable[1][2] = 1;
		priorityTable[2][0] = -1;
		priorityTable[2][1] = -1;
		priorityTable[2][2] = 2;
	}

	public void run() {
		if (query.processType != 0) {// 不是insert操作
			Queue<Object> tokens = query.tokenQueue;
			int status = 0;
			while (status != 2) {
				String nowToken = (String) tokens.peek();
				if (!nowToken.equals("and") && !nowToken.equals("or")
						&& !nowToken.equals("#")) {
					// 处理数栈
					String attr = (String) tokens.poll();
					String c = (String) tokens.poll();
					Object value = tokens.poll();
					if (c.equals("=")) {
						eq(attr, value);
					} else if (c.equals(">")) {
						gt(attr, value);
					} else if (c.equals("<")) {
						lt(attr, value);
					}
				} else {
					// 处理操作符栈
					String op1 = (String) tokens.peek();
					String op2 = ops.peek();
					status = priorityTable[opIdx(op1)][opIdx(op2)];
					if (status == -1) {
						Set<Row> set1 = ds.pop();
						Set<Row> set2 = ds.pop();
						ops.pop();
						if (op2.equals("and")) {
							set1.retainAll(set2);
						} else if (op2.equals("or")) {
							set1.addAll(set2);
						}
						ds.push(set1);
					} else if (status == 1) {
						tokens.poll();
						ops.push(op1);
					}
				}
			}
			Set<Row> result = ds.pop();
			if (query.processType == 1) {// update操作
				Map<String, Object> attrValue = query.attrValue;
				for (Row row : result) {
					row.write(attrValue);
				}
			} else if (query.processType == 2) {// delete操作
				Table table = query.table;
				for (Row row : result) {
					Map<String, Object> r = row.read();
					// 删除table.dtc中的数据
					for (String attr : r.keySet()) {
						if (table.getDTC().containsKey(attr)) {
							Map<Object, Set<Row>> kv = table.getDTC().get(attr).kv;
							Object value = r.get(attr);
							kv.get(value).remove(row);
							if (kv.get(value).isEmpty())
								kv.remove(value);
						}
					}
					// 删除table.table中的数据
					table.getTable().remove(row);
				}
			} else if (query.processType == 3) {// fetchAll操作
				List<Map<String, Object>> r = new ArrayList<Map<String, Object>>();
				for (Row row : result) {
					r.add(row.read());
				}
				out.out(r);
			}
		} else {// insert操作
			Table table = query.table;
			Map<String, Object> attrValue = query.attrValue;
			// 向table.table中添加数据
			Row row = new Row(attrValue, table);
			table.getTable().add(row);
			// 向table.dtc中添加数据
			for (String attr : attrValue.keySet()) {
				if (table.getDTC().containsKey(attr)) {
					Object value = attrValue.get(attr);
					Map<Object, Set<Row>> kv = table.getDTC().get(attr).kv;
					if (!kv.containsKey(value)) {
						Set<Row> set = new HashSet<Row>();
						kv.put(value, set);
					}
					kv.get(value).add(row);
				}
			}
		}

		// 向持久化线程中添加任务
		if (query.processType == 0 || query.processType == 1
				|| query.processType == 2) {
			PersistenceTask pt = new PersistenceTask(query.queryString);
			DataPersistenceThread.getInstance().addTask(pt);
		}
	}

	private void eq(String attr, Object value) {
		Set<Row> temp = new HashSet<Row>();
		Table table = query.table;

		// 检查table中是否有以attr作为key的缓存
		if (!table.getDTC().containsKey(attr)) {// 需要生成这种拆分方式的缓存
			KeyValue kv = new KeyValue();
			for (Row row : table.getTable()) {
				Map<String, Object> r = row.read();
				Object key = r.get(attr);
				if (kv.kv.containsKey(key)) {
					kv.kv.get(key).add(row);
				} else {
					Set<Row> set = new HashSet<Row>();
					set.add(row);
					kv.kv.put(key, set);
				}
			}
			table.getDTC().put(attr, kv);
		}

		Set<Row> t = table.getDTC().get(attr).kv.get(value);// 得到eq这个where子句搜索到的集合
		if (t != null) {
			for (Row row : t) {
				temp.add(row);
			}
		}

		ds.push(temp);
	}

	private void gt(String attr, Object value) {
		Set<Row> temp = new HashSet<Row>();
		Table table = query.table;

		if (value instanceof Integer || value instanceof Long
				|| value instanceof Float || value instanceof Double) {
			for (Row row : table.getTable()) {
				Map<String, Object> r = row.read();
				if (value instanceof Integer) {
					if ((Integer) r.get(attr) > (Integer) value) {
						temp.add(row);
					}
				}
				if (value instanceof Long) {
					if ((Long) r.get(attr) > (Long) value) {
						temp.add(row);
					}
				}
				if (value instanceof Float) {
					if ((Float) r.get(attr) > (Float) value) {
						temp.add(row);
					}
				}
				if (value instanceof Double) {
					if ((Double) r.get(attr) > (Double) value) {
						temp.add(row);
					}
				}
			}
		} else {
			System.out.println("gt 函数的输入参数类型错误");
			System.out.println("错误的queryString：" + query.queryString);
			ds.push(null);
		}
		ds.add(temp);
	}

	private void lt(String attr, Object value) {
		Set<Row> temp = new HashSet<Row>();
		Table table = query.table;

		if (value instanceof Integer || value instanceof Long
				|| value instanceof Float || value instanceof Double) {
			for (Row row : table.getTable()) {
				Map<String, Object> r = row.read();
				if (value instanceof Integer) {
					if ((Integer) r.get(attr) < (Integer) value) {
						temp.add(row);
					}
				}
				if (value instanceof Long) {
					if ((Long) r.get(attr) < (Long) value) {
						temp.add(row);
					}
				}
				if (value instanceof Float) {
					if ((Float) r.get(attr) < (Float) value) {
						temp.add(row);
					}
				}
				if (value instanceof Double) {
					if ((Double) r.get(attr) < (Double) value) {
						temp.add(row);
					}
				}
			}
		} else {
			System.out.println("lt 函数的输入参数类型错误");
			System.out.println("错误的queryString：" + query.queryString);
			ds.push(null);
		}
		ds.add(temp);
	}

	private int opIdx(String op) {
		if (op.equals("and")) {
			return 0;
		} else if (op.equals("or")) {
			return 1;
		} else if (op.equals("#")) {
			return 2;
		} else {
			return -1;
		}
	}
}
