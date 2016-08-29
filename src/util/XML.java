package util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class XML {
	public Map<String, String> mysqlConf() {
		Map<String, String> r = new HashMap<String, String>();
		SAXReader reader = new SAXReader();
		try {
			Document document = reader.read(new File("conf/conf.xml"));
			Element root = document.getRootElement();
			Element mysql = root.element("mysql");
			String url = mysql.element("url").getText();
			String user = mysql.element("user").getText();
			String password = mysql.element("password").getText();
			r.put("url", url);
			r.put("user", user);
			r.put("password", password);
		} catch (DocumentException e) {
			e.printStackTrace();
			return null;
		}
		return r;
	}
	
	public List<String> tableConf() {
		List<String> r = new ArrayList<String>();
		SAXReader reader = new SAXReader();
		try {
			Document document = reader.read(new File("conf/conf.xml"));
			Element root = document.getRootElement();
			Element table = root.element("table");

			@SuppressWarnings("unchecked")
			List<Element> names = table.elements("name");
			for(int i=0; i<names.size(); ++i){
				r.add(names.get(i).getText());
			}
		} catch (DocumentException e) {
			e.printStackTrace();
			return null;
		}
		return r;
	}
	
	public static void main(String[] args) {
		new XML().mysqlConf();
	}
}
