package backThread;

import java.util.LinkedList;
import java.util.Queue;

public class DataPersistenceThread {
	private static DataPersistenceThread instance = new DataPersistenceThread();
	public static DataPersistenceThread getInstance() {
		return instance;
	}
	private Queue<PersistenceTask> tasks;
	private Worker worker;
	private DataPersistenceThread(){
		tasks = new LinkedList<PersistenceTask>();
		worker = new Worker();
		worker.start();
	};
	
	class Worker extends Thread {// 正式员工线程
		public void run() {
			while (true) {
				Runnable task;
				synchronized (tasks) {
					while (tasks.isEmpty()) {
						try {
							tasks.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					task = tasks.poll();
				}
				task.run();
			}
		}
	}
	
	public void addTask(PersistenceTask task) {
		synchronized (tasks) {
			tasks.offer(task);
			tasks.notify();
		}
	}
}
