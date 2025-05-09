package de.zeroco.service;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.zeroco.util.*;

public class IndexMgmt {
	
	public static Map<String, Object> create(String schema, String tableName, String column, String index) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verification(schema, tableName, column, index);
		if (!message.isEmpty()) {
			response.put("success", false);
			response.put("message", message);
			return response;
		}
		Connection con = null;
		try {
			con = DbUtil.getConnection(schema);
			if (!schema.trim().isEmpty() && !DbUtil.schemaExists(con, schema)) {
				Util.addMessage(response, message, false, "Schema does not exist.");
				response.put("message", message);
				return response;
			}
			if (DbUtil.isTableExists(con, schema, tableName)) {
				if (DbUtil.columnExists(con, tableName, column, schema)) {
					if (!DbUtil.isIndexExists(con, schema, tableName, index)) {
						if (DbUtil.executeQuery(con, schema, QueryGenerator.createIndex(schema, tableName, column, index))) {
							Util.addMessage(response, message, true, "Index created on column successfully.");
						} else {
							Util.addMessage(response, message, false, "Failed to create index.");
						}
					} else {
						Util.addMessage(response, message, false, "Index already exists.");
					}
				} else {
					Util.addMessage(response, message, false, "Column does not exist.");
				}
			} else {
				Util.addMessage(response, message, false, "Table does not exist.");
			}
		} catch (Exception e) {
			Util.addMessage(response, message, false, "Error occurred: " + e.getMessage());
		} finally {
			DbUtil.closeConnection(con);
		}
		response.put("message", message);
		return response;
	}
	
	public static Map<String, Object> create(String tableName, String column, String index) {
		return create("", tableName, column, index);
	}
	
	public static Map<String, Object> drop(String schema, String tableName, String column, String index) {
	    Map<String, Object> response = new HashMap<>();
	    List<String> message = Util.verification(schema, tableName, index, column);
	    if (!message.isEmpty()) {
	        response.put("success", false);
	        response.put("message", message);
	        return response;
	    }
	    Connection con = null;
	    try {
	        con = DbUtil.getConnection();
	        if (!schema.trim().isEmpty() && !DbUtil.schemaExists(con, schema)) {
	            Util.addMessage(response, message, false, "Schema does not exist.");
	            response.put("message", message);
	            return response;
	        }
	        if (DbUtil.isTableExists(con, schema, tableName)) {
	            if (DbUtil.columnExists(con, tableName, column, schema)) {
	                if (DbUtil.executeQuery(con, schema, QueryGenerator.dropIndex(schema, tableName, index))) {
	                    Util.addMessage(response, message, true, "Index deleted successfully.");
	                } else {
	                    Util.addMessage(response, message, false, "Failed to delete index.");
	                }
	            } else {
	                Util.addMessage(response, message, false, "Column does not exist.");
	            }
	        } else {
	            Util.addMessage(response, message, false, "Table does not exist.");
	        }
	    } catch (Exception e) {
	        Util.addMessage(response, message, false, "Error occurred: " + e.getMessage());
	    } finally {
	    	DbUtil.closeConnection(con);
	    }
	    response.put("message", message);
	    return response;
	}

	public static Map<String, Object> drop(String tableName, String column, String index) {
		return drop("", tableName, column, index);
	}
	
	public static Map<String, Object> createUnique(String schema, String tableName, String column, String index) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verification(schema, tableName, column, index);
		if (!message.isEmpty()) {
			response.put("success", false);
			response.put("message", message);
			return response;
		}
		Connection con = null;
		try {
			con = DbUtil.getConnection();
			if (!schema.trim().isEmpty() && !DbUtil.schemaExists(con, schema)) {
				Util.addMessage(response, message, false, "Schema does not exist.");
				response.put("message", message);
				return response;
			}
			if (DbUtil.isTableExists(con, schema, tableName)) {
				if (DbUtil.columnExists(con, tableName, column, schema)) {
					if (!DbUtil.isIndexExists(con, schema, tableName, index)) {
						if (DbUtil.executeQuery(con, schema, QueryGenerator.createUniqueIndex(schema, tableName, column, index))) {
							Util.addMessage(response, message, true, "Index created successfully.");
						} else {
							Util.addMessage(response, message, false, "Failed to create index.");
						}
					} else {
						Util.addMessage(response, message, false, "Index already exists.");
					}
				} else {
					Util.addMessage(response, message, false, "Column does not exist.");
				}
			} else {
				Util.addMessage(response, message, false, "Table does not exist.");
			}
		} catch (Exception e) {
			Util.addMessage(response, message, false, "Error occurred: " + e.getMessage());
		} finally {
			DbUtil.closeConnection(con);
		}
		response.put("message", message);
		return response;
	}
	
	public static void main(String[] args) {
//		Connection con = null;
//		try {
//		    con = DbUtil.getConnection();
//		} catch (Exception e) {
//		    e.printStackTrace();
//		} finally {
//		    try {
//		        if (con != null) con.close();
//		    } catch (Exception e) {
//		        e.printStackTrace();
//		    }
//		}
//		System.out.println(create("book", "book_name", "name"));
//		System.out.println(create("book", "roll_number", "indexx"));
//		System.out.println(drop("book", "roll_number", "indexx"));
//		System.out.println(drop("zc_map", "banana", "name", "name"));
	}
}
