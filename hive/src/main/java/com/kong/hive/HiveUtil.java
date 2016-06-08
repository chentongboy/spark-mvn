package com.kong.hive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveUtil {

	public static void createTable(String hiveql) throws SQLException {
		System.out.println("hiveql=" + hiveql);
		Connection con = HiveConnection.getHiveConn();
		PreparedStatement stmt = con.prepareStatement(hiveql);
		ResultSet res = stmt.executeQuery(hiveql);
	}

	/**
	 * 返回hiveql查询结果
	 * @param hiveql
	 * @return ResultSet
	 * @throws SQLException
	 */
	public static ResultSet queryHive(String hiveql) throws SQLException {
		System.out.println("hiveql=" + hiveql);
		Connection con = HiveConnection.getHiveConn();
		PreparedStatement stmt = con.prepareStatement(hiveql);
		ResultSet res = stmt.executeQuery(hiveql);
		return res;
	}
}
