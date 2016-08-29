package base;

import java.util.List;
import java.util.Map;

public class OutPutCallBack {
	public void out(List<Map<String, Object>> r) {
		for (int i = 0; i < r.size(); ++i) {
			for(String key : r.get(i).keySet()) {
				System.out.print(key+":"+r.get(i).get(key)+" ");
			}
			System.out.println();
		}
	}
}
