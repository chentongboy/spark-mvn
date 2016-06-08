package com.kong.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class HiveConnection {

	private static Connection connHive = null;

	public static Connection getHiveConn() throws SQLException {
		if (connHive == null) {
			try {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(1);
			}
			// 此处的用户名和密码用不到，可以为空
			connHive = DriverManager.getConnection("jdbc:hive2://Master:10000/hive", "root","" );
		}
		return connHive;
	}

	public static void closeHive() throws SQLException {
		if (connHive != null)
			connHive.close();
	}
}
