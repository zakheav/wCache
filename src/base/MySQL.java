package base;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import util.XML;

public class MySQL {
	private Connection conn;
	private static String url;
	private static String user;
	private static String password;
	private static String driverClassName = "com.mysql.jdbc.Driver";
	private static MySQL instance = new MySQL();
	public static MySQL getInstance() {
		return instance;
	}
	private MySQL() {
		// 读取xml文件生成url，user，password
		Map<String, String> conf = new XML().mysqlConf();
		url = conf.get("url");
		user = conf.get("user");
		password = conf.get("password");
		try {
			Class.forName(MySQL.driverClassName);
			conn = DriverManager.getConnection(MySQL.url, MySQL.user, MySQL.password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<Map<String, Object>> executeQuery(String queryString) {
		Statement stmt = null;
		ArrayList<Map<String, Object>> result = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(queryString);// 可能出现问题（链接失效）
			result = resultSet_to_obj(rs);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} 
		return result;
	}
	
	public Boolean executeUpdate(String queryString) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(queryString);// 可能出现问题（链接失效）
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} 
		return true;
	}

	private ArrayList<Map<String, Object>> resultSet_to_obj(ResultSet r) {// 从resultSet变为List对象
		ArrayList<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			ResultSetMetaData rsmd = r.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			while (r.next()) {
				Map<String, Object> row = new HashMap<String, Object>();
				for (int i = 1; i <= numberOfColumns; ++i) {
					String name = rsmd.getColumnName(i);
					Object value = r.getObject(name);
					row.put(name, value);
				}
				result.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
