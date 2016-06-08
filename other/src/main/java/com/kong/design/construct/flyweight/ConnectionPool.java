package com.kong.design.construct.flyweight;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Vector;

/**
 * 享元模式的主要目的是实现对象的共享，即共享池，当系统中对象多的时候可以减少内存的开销，通常与工厂模式一起使用。
 * 通过连接池的管理，实现了数据库连接的共享，不需要每一次都重新创建连接，节省了数据库重新创建的开销，提升了系统的性能！
 * Created by kong on 2016/4/22.
 */
public class ConnectionPool {
    private Vector<Connection> pool;

    private String url = "jdbc:mysql://localhost:3306/test";
    private String userName = "root";
    private String password = "root";
    private String driverClassName = "com.mysql.jdbc.Driver";

    private int poolSize = 100;
    private static ConnectionPool instance = null;
    Connection conn = null;

    //构造方法初始化连接池
    private ConnectionPool() {
        pool = new Vector<Connection>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            try {
                Class.forName(driverClassName);
                conn = DriverManager.getConnection(url);
                pool.add(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //返回连接到连接池
    public synchronized void release() {
        pool.add(conn);
    }

    /*返回连接池中的一个数据库连接*/
    public synchronized Connection getConnection() {
        if (pool.size() > 0) {
            Connection conn = pool.get(0);
            pool.remove(conn);
            return conn;
        } else return null;
    }
}
