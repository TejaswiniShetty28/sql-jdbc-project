package com.zeroco.integration;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;

public class MySqlConnection {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/school";
        String user = "root";
        String password = "the@123";
        String sql = "SELECT class_room, class_id FROM class";
        Connection connection = null; 
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(url, user, password); 
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/school", "root", "the@123"); 
            System.out.println("Connection established successfully.");
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String classRoom = resultSet.getString("class_room");
                int classId = resultSet.getInt("class_id");
                System.out.println("Class Room: " + classRoom + ", Class ID: " + classId);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (stmt != null) stmt.close();
                if (connection != null) connection.close();
                System.out.println("Resources closed.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
