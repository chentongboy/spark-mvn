package com.kong.hive;

import java.sql.ResultSet;
import java.sql.SQLException;

public class exeHiveQL {
	public static void main(String[] args) {
		exeHiveQL exeHiveQL = new exeHiveQL();
		exeHiveQL.searchLogInfo(args);
	}

	// searchLogInfo
	public void searchLogInfo(String[] args) {
		try {
			// 查询有用的信息，这里依据日期和日志级别过滤信息
			ResultSet res = HiveUtil.queryHive("select * from hive.people");


			showResults(res);

			// 最后关闭此次会话的hive连接
			HiveConnection.closeHive();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// showResults
	private void showResults(ResultSet res) throws SQLException {
		while (res.next()) {
			String name = res.getString(1);
			String age = res.getString(2);
			
			System.out.println(name + "	" + age);
		}
	}

}
