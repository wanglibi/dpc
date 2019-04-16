package com.cmsz.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //192.168.1.27:3306/dpc
    //root
    //123456
    public synchronized static Connection getConnection(String url,String user,String password) {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0;i < 5;i ++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://"+url+"?characterEncoding=utf8&useSSL=true&serverTimezone=GMT%2B8",
                            user,
                            password
                    );
                    connectionQueue.push(conn);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}