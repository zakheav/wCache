package backThread;

import java.util.LinkedList;
import java.util.Queue;

public class QueryThread {
	private static QueryThread instance = new QueryThread();

	public static QueryThread getInstance() {
		return instance;
	}

	private Queue<Object> tasks;
	private Worker worker;

	private QueryThread() {
		tasks = new LinkedList<Object>();
		worker = new Worker();
		worker.start();
	};

	class Worker extends Thread {// 正式员工线程
		public void run() {
			while (true) {
				Object temp;
				Runnable task;
				synchronized (tasks) {
					while (tasks.isEmpty()) {
						try {
							tasks.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					temp = tasks.poll();
				}
				if (temp instanceof QueryTask) {// 普通的查询
					task = (Runnable) temp;
					task.run();
				} else if (temp instanceof TrasactionTask) {// 事务查询
					for(int i=0; i<((TrasactionTask)temp).queryTaskList.size(); ++i) {
						task = ((TrasactionTask)temp).queryTaskList.get(i);
						task.run();
					}
				}
			}
		}
	}

	public void addTask(Object task) {
		synchronized (tasks) {
			tasks.offer(task);
			tasks.notify();
		}
	}
}
