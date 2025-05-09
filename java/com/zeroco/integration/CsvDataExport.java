package com.zeroco.integration;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

public class CsvDataExport {
    private static final String CSV_FILE_PATH = "C:\\Users\\shett\\file\\bkp\\family.csv";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/family";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "the@123";

    public static void importToDb() {
        String insertQuery = "INSERT INTO details (nick_name, name, earning, role, dob) VALUES (?, ?, ?, ?, ?)";
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             CSVReader csvReader = new CSVReader(new FileReader(CSV_FILE_PATH));
             PreparedStatement insertStmt = connection.prepareStatement(insertQuery)) {
            List<String[]> records = csvReader.readAll();
            records.stream().skip(1).forEach(record -> {
                try {
                    if (!isDuplicate(connection, record[0])) {  
                        insertStmt.setString(1, record[0]); 
                        insertStmt.setString(2, record[1]);
                        insertStmt.setString(3, record[2]); 
                        insertStmt.setString(4, record[3]);
                        insertStmt.setString(5, record[4]); 
                        insertStmt.executeUpdate();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException | SQLException | CsvException e) {
            e.printStackTrace();
        } 
    }

//    private static boolean isDuplicate(Connection connection, String nickName) throws SQLException {
//        String checkQuery = "SELECT COUNT(*) FROM details WHERE nick_name = ?";
//        try (PreparedStatement checkStmt = connection.prepareStatement(checkQuery)) {
//            checkStmt.setString(1, nickName);
//            ResultSet resultSet = checkStmt.executeQuery();
//            resultSet.next();
//            return resultSet.getInt(1) > 0; 
//        }
//    }
    
    private static boolean isDuplicate(Connection connection, String nickName) throws SQLException {
        String checkQuery = "SELECT 1 FROM details WHERE nick_name = ?";
        try (PreparedStatement checkStmt = connection.prepareStatement(checkQuery)) {
            checkStmt.setString(1, nickName);
            try (ResultSet resultSet = checkStmt.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    public static void main(String[] args) {
        importToDb();
    }
}
