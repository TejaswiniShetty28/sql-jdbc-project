package de.zeroco.service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.zeroco.util.*;

public class TableMgmt {
	
	public static Map<String, Object> create(String schema, String tableName, List<Map<String, String>> columns) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verifyDetails(schema, tableName, columns);
		if (!message.isEmpty()) {
			response.put("success", false);
			response.put("message", message);
			return response;
		}
		Connection con = null;
		try {
		    con = DbUtil.getConnection();
		    try {
				if (!Util.isBlank(schema) && !DbUtil.schemaExists(con, schema)) {
					DbUtil.createSchema(con, schema);
				}
				if (!DbUtil.isTableExists(con, schema, tableName)) {
					if (DbUtil.executeQuery(con, schema, QueryGenerator.createTable(schema, tableName, columns))) {
						Util.addMessage(response, message, true, "Table created successfully.");
					} else {
						Util.addMessage(response, message, false, "Failed to create table.");
					}
				} else {
					Util.addMessage(response, message, false, "Table already exists.");
				}
			} catch (Exception e) {
				Util.addMessage(response, message, false, e.getMessage());
			}
		} catch (Exception e) {
		    e.printStackTrace();
		} finally {
		    DbUtil.closeConnection(con);
		}
		response.put("message", message);
		return response;
	}

	public static Map<String, Object> create(String tableName, List<Map<String, String>> columns) {
		return create("", tableName, columns);
	}
	
	public static Map<String, Object> drop(String schema, String tableName) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verifyDetails(schema, tableName);
		if (!message.isEmpty()) {
			response.put("success", false);
			response.put("message", message);
			return response;
		}
		Connection con = null;
		try {
			con = DbUtil.getConnection();
			if (!Util.isBlank(schema) && !DbUtil.schemaExists(con, schema)) {
				Util.addMessage(response, message, false, "Schema does not exist.");
				response.put("message", message);
				return response;
			}
			if (DbUtil.isTableExists(con, schema, tableName)) {
				if (DbUtil.executeQuery(con, schema, QueryGenerator.dropTable(schema, tableName))) {
					Util.addMessage(response, message, true, "Table deleted successfully.");
				} else {
					Util.addMessage(response, message, false, "Failed to delete table.");
				}
			} else {
				Util.addMessage(response, message, false, "Table does not exist.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtil.closeConnection(con);
		}
		response.put("message", message);
		return response;
	}
	
	public static Map<String, Object> drop(String tableName) {
		return drop("", tableName);
	}
	
	public static Map<String, Object> rename(String schema, String newTableName, String oldTableName) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verifyTableNames(schema, newTableName, oldTableName);
		if (!message.isEmpty()) {
			response.put("success", false);
			response.put("message", message);
			return response;
		}
		Connection con = null;
		try {
			con = DbUtil.getConnection();
			if (!Util.isBlank(schema) && !DbUtil.schemaExists(con, schema)) {
				Util.addMessage(response, message, false, "Schema does not exist.");
				response.put("message", message);
				return response;
			}
			if (DbUtil.isTableExists(con, schema, oldTableName)) {
				if (!DbUtil.isTableExists(con, schema, newTableName)) {
					if (DbUtil.executeQuery(con, schema, QueryGenerator.renameTable(schema, newTableName, oldTableName))) {
						Util.addMessage(response, message, true, "Renamed table successfully.");
					} else {
						Util.addMessage(response, message, false, "Failed to rename table.");
					}
				} else {
					Util.addMessage(response, message, false, "Table with the new name already exists.");
				}
			} else {
				Util.addMessage(response, message, false, "Old table does not exist.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtil.closeConnection(con);
		}
		response.put("message", message);
		return response;
	}

	public static Map<String, Object> rename(String newtableName, String oldTableName) {
		return rename("", newtableName, oldTableName);
	}
	
	public static Map<String, Object> truncate(String schema, String tableName) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verifyDetails(schema, tableName);
		if (!message.isEmpty()) {
			response.put("success", false);
			response.put("message", message);
			return response;
		}
		Connection con = null;
		try {
			con = DbUtil.getConnection();
			if (!Util.isBlank(schema) && !DbUtil.schemaExists(con, schema)) {
				Util.addMessage(response, message, false, "Schema does not exist.");
				response.put("message", message);
				return response;
			}
			if (DbUtil.isTableExists(con, schema, tableName)) {
				if (DbUtil.executeQuery(con, schema, QueryGenerator.truncateTable(schema, tableName))) {
					Util.addMessage(response, message, true, "Successfully truncated data in table.");
				} else {
					Util.addMessage(response, message, false, "Failed to truncated data in table.");
				}
			} else {
				Util.addMessage(response, message, false, "Table does not exist.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtil.closeConnection(con);
		}
		response.put("message", message);
		return response;
	}

	public static Map<String, Object> truncate(String tableName) {
		return truncate("", tableName);
	}
	
	public static void main(String[] args) {
		//******** create method
		List<Map<String, String>> params = new ArrayList<>();
		Map<String, String> col1 = new HashMap<>();
		col1.put("name", "id");
		col1.put("type", "INT");
		col1.put("required", "true");
		Map<String, String> col2 = new HashMap<>();
		col2.put("name", "name");
		col2.put("type", "text");
//		col2.put("size", "40");
//		col2.put("default_value", "'unknown'");
		col2.put("required", "false");
		Map<String, String> col3 = new HashMap<>();
		col3.put("name", "email");
		col3.put("type", "text");
//		col3.put("size", "30");
		col3.put("required", "true");
		Map<String, String> col4 = new HashMap<>();
		col4.put("name", "phone");
		col4.put("type", "text");
//		col4.put("default_value", "0");
		col4.put("required", "false");
		Map<String, String> col5 = new HashMap<>();
		col5.put("name", "address");
		col5.put("type", "text");
//		col5.put("size", "16"); 
		col5.put("required", "true");
//		Map<String, String> col6 = new HashMap<>();
//		col6.put("name", "pincode");
//		col6.put("type", "text");
////		col6.put("size", "255"); 
//		col6.put("required", "false");
		params.add(col1);
//		params.add(col5);
		params.add(col2);
		params.add(col3);
		params.add(col4);
//		params.add(col6);
		System.out.println(truncate("zc_core", "user_name"));
		//// *******
//		System.out.println(drop("file"));
//		///// ***** alter table
//		System.out.println(rename("russia", "green", "zxcvb"));
		///***** truncate
//		System.out.println(truncate("virat"));
	}
}

