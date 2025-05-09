package com.zeroco.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Employee {
	
	public static Connection getConnection() throws SQLException {
        String url = "jdbc:mysql://localhost:3306/employee";
        String user = "root";
        String password = "the@123";
        return DriverManager.getConnection(url, user, password);
    }
	
	public static void executeQuery() {
		List<Object[]> list = new ArrayList<Object[]>();
        String sql = "SELECT id, first_name, last_name, department, salary, phone_number FROM employees";
        Connection connection = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();  
//            System.out.println("Connection established successfully.");
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
            	Object[] array = new Object[6];
                array[0] = resultSet.getInt("id");
                array[1] = resultSet.getString("first_name");
                array[2] = resultSet.getString("last_name");
                array[3] = resultSet.getString("department");
                array[4] = resultSet.getInt("salary");
                array[5] = resultSet.getString("phone_number");
                list.add(array);
//                System.out.println("ID" + id + "First Name" + firstName + "Last Name" + lastName + "Department" + department + "Salary" + salary + "Phone Number" + phoneNumber);
            }
            for (Object[] employee : list) {
                for (Object i : employee) {
                	System.out.println(i);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (stmt != null) stmt.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
	
	public static String delete(int employeeId) {
	    String checkSql = "SELECT COUNT(*) FROM employees WHERE id = ?";
	    String deleteSql = "DELETE FROM employees WHERE id = ?";
	    Connection connection = null;
	    PreparedStatement checkStmt = null;
	    PreparedStatement deleteStmt = null;
	    ResultSet resultSet = null;
	    try {
	        connection = getConnection();  
	        checkStmt = connection.prepareStatement(checkSql);
	        checkStmt.setInt(1, employeeId);
	        resultSet = checkStmt.executeQuery();
	        if (resultSet.next() && resultSet.getInt(1) > 0) {
	            deleteStmt = connection.prepareStatement(deleteSql);
	            deleteStmt.setInt(1, employeeId);
	            int rowsAffected = deleteStmt.executeUpdate();
	            if (rowsAffected > 0) {
	                return "Employee with id " + employeeId + " deleted successfully.";
	            } else {
	                return "Failed to delete employee with id " + employeeId;
	            }
	        } else {
	            return "Employee with id " + employeeId + " not found.";
	        }
	    } catch (SQLException e) {
	        return "Error occurred: " + e.getMessage();
	    } finally {
	        try {
	            if (resultSet != null) resultSet.close();
	            if (checkStmt != null) checkStmt.close();
	            if (deleteStmt != null) deleteStmt.close();
	            if (connection != null) connection.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }
	}
	
	public static String update(int id, String salary, String phoneNumber) {
	    String checkSql = "SELECT COUNT(*) FROM employees WHERE id = ?";
	    String updateSql = "UPDATE employees SET salary = ?, phone_number = ? WHERE id = ?";
	    Connection connection = null;
	    PreparedStatement checkStmt = null;
	    PreparedStatement updateStmt = null;
	    ResultSet resultSet = null;
	    try {
	        connection = getConnection();
	        checkStmt = connection.prepareStatement(checkSql);
	        checkStmt.setInt(1, id);
	        resultSet = checkStmt.executeQuery();
	        if (resultSet.next() && resultSet.getInt(1) > 0) {
	            updateStmt = connection.prepareStatement(updateSql);
	            updateStmt.setString(1, salary);
	            updateStmt.setString(2, phoneNumber);
	            updateStmt.setInt(3, id);
	            int rowsAffected = updateStmt.executeUpdate();
	            if (rowsAffected > 0) {
	                return "Employee with ID " + id + " updated successfully.";
	            } else {
	                return "Failed to update employee with ID " + id + ".";
	            }
	        } else {
	            return "Employee with ID " + id + " not found.";
	        }
	    } catch (SQLException e) {
	        e.printStackTrace();
	        return "Error occurred while updating employee.";
	    } finally {
	        try {
	            if (resultSet != null) resultSet.close();
	            if (checkStmt != null) checkStmt.close();
	            if (updateStmt != null) updateStmt.close();
	            if (connection != null) connection.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }
	}
	
	public static void insert(String firstName, String lastName, String department, int salary, String phoneNumber) {
	    String sql = "INSERT INTO employees (first_name, last_name, department, salary, phone_number) VALUES (?, ?, ?, ?, ?)";
	    Connection connection = null;
	    PreparedStatement pstmt = null;
	    try {
	        connection = getConnection();  
	        pstmt = connection.prepareStatement(sql);
	        pstmt.setString(1, firstName);
	        pstmt.setString(2, lastName);
	        pstmt.setString(3, department);
	        pstmt.setInt(4, salary);
	        pstmt.setString(5, phoneNumber);
	        pstmt.executeUpdate();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            if (pstmt != null) pstmt.close();
	            if (connection != null) connection.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }
	}

	public static void main(String[] args) {
//		executeQuery();
		System.out.println("\n\texecute operation done");
		System.out.println(delete(2));
		System.out.println(update(3, "3500", "876543"));
//		insert("Saritha", "Molugu", "EEE", 6700, "6543219878");
		executeQuery();
	}
}
