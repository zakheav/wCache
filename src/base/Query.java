package base;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backThread.QueryTask;
import backThread.QueryThread;

public class Query {// Query和Table是一对相互引用的对象
	public Table table = null;

	// 每次搜索过程的暂存值
	public String queryString = "";
	public Queue<Object> tokenQueue = new LinkedList<Object>();// 存储where子句中的符号序列
	public Map<String, Object> attrValue = null;
	public int processType = 0;// 0调用insert函数 1调用update函数 2调用delete函数 3select

	public Query(Table t) {
		table = t;
	}

	public Query select() {// 不做投影操作
		processType = 3;
		queryString += "select * from " + table.name;
		return this;
	}

	public Query update(Map<String, Object> params) {
		attrValue = params;
		processType = 1;
		// 拼接sql语句
		queryString += "update " + table.name + " set ";
		int count = 0;
		int length = attrValue.size();
		for (String key : attrValue.keySet()) {
			++count;
			Object value = attrValue.get(key);
			this.queryString += key + " = ";
			if (value instanceof Integer || value instanceof Long || value instanceof Float
					|| value instanceof Double) {
				if (count == length)
					this.queryString += value + " ";
				else
					this.queryString += value + ", ";
			} else if (value instanceof String) {
				if (count == length)
					this.queryString += "'" + value + "' ";
				else
					this.queryString += "'" + value + "', ";
			} else {
				System.out.println("未知的数据类型");
				return null;
			}
		}
		return this;
	}

	public Query insert(Map<String, Object> params) {
		attrValue = params;
		processType = 0;
		// 拼接sql语句
		queryString += "insert into " + table.name + " ( ";
		int count = 0;
		int length = attrValue.size();
		for (String key : attrValue.keySet()) {
			++count;
			if (count == length)
				queryString += key + " )";
			else
				queryString += key + ",";
		}
		queryString += " values ( ";
		count = 0;
		for (String key : attrValue.keySet()) {
			++count;
			Object value = attrValue.get(key);
			if (value instanceof Integer || value instanceof Long || value instanceof Float
					|| value instanceof Double) {
				if (count == length)
					this.queryString += value + " )";
				else
					this.queryString += value + ", ";
			} else if (value instanceof String) {
				if (count == length)
					this.queryString += "'" + value + "' )";
				else
					this.queryString += "'" + value + "', ";
			} else {
				System.out.println("未知的数据类型");
				return null;
			}
		}
		return this;
	}

	public Query delete() {
		processType = 2;
		// 拼接sql语句
		queryString += "delete from " + table.name;
		return this;
	}

	public Query where(String attr) {
		queryString += " where " + attr;
		tokenQueue.offer(attr);
		return this;
	}

	public Query andWhere(String attr) {
		queryString += " and " + attr;
		tokenQueue.offer("and");
		tokenQueue.offer(attr);
		return this;
	}

	public Query orWhere(String attr) {
		queryString += " or " + attr;
		tokenQueue.offer("or");
		tokenQueue.offer(attr);
		return this;
	}

	public Query eq(Object value) {// equal
		tokenQueue.offer("=");
		tokenQueue.offer(value);
		// 拼接sql语句
		if (value instanceof Integer || value instanceof Long || value instanceof Float || value instanceof Double) {
			this.queryString += " = " + value;
		} else if (value instanceof String) {
			this.queryString += " = '" + value + "'";
		} else {
			System.out.println("未知的数据类型");
			return null;
		}
		return this;
	}

	public Query lt(Object value) {// little than
		if (value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
			this.queryString += " < " + value;
			tokenQueue.offer("<");
			tokenQueue.offer(value);
		} else {
			System.out.println("lt函数不接受非数字参数");
			return null;
		}
		return this;
	}

	public Query bt(Object value) {// bigger than
		if (value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
			this.queryString += " > " + value;
			tokenQueue.offer(">");
			tokenQueue.offer(value);
		} else {
			System.out.println("bt 不接受非数字参数");
			return null;
		}
		return this;
	}

	public void execute(OutPutCallBack opcb) {
		tokenQueue.offer("#");
		// 把Query对象加入到任务队列
		QueryTask qt = new QueryTask(this, opcb);
		QueryThread.getInstance().addTask(qt);
	}

	public void addToTrasaction(Trasaction t, OutPutCallBack opcb){
		tokenQueue.offer("#");
		t.queryList.add(this);
		t.callBackList.add(opcb);
	}
}
