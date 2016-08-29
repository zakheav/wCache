package base;

import java.util.ArrayList;
import java.util.List;

import backThread.QueryTask;
import backThread.QueryThread;
import backThread.TrasactionTask;

public class Trasaction {

	public List<Query> queryList = new ArrayList<Query>();
	public List<OutPutCallBack> callBackList = new ArrayList<OutPutCallBack>();

	public void trasactionExecute() {// 创造一个TrasactionTask
		TrasactionTask tt = new TrasactionTask();
		for (int i = 0; i < queryList.size(); ++i) {
			QueryTask qt = new QueryTask(queryList.get(i), callBackList.get(i));
			tt.queryTaskList.add(qt);
		}
		QueryThread.getInstance().addTask(tt);
	}
}
