package de.zeroco.service;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.zeroco.util.*;

public class ColumnMgmt {

	public static Map<String, Object> add(String schema, String tableName, Map<String, String> column) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verifyDetails(schema, tableName, column);
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
				if (!DbUtil.columnExists(con, tableName, column.get("name"), schema)) {
					if (DbUtil.executeQuery(con, schema, QueryGenerator.addColumn(schema, tableName, column))) {
						Util.addMessage(response, message, true, "Successfully added column.");
					} else {
						Util.addMessage(response, message, false, "Failed to add column.");
					}
				} else {
					Util.addMessage(response, message, false, "Column already exists.");
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

	public static Map<String, Object> add(String tableName, Map<String, String> column) {
		return add("", tableName, column);
	}
	
	public static Map<String, Object> drop(String schema, String tableName, String column) {
	    Map<String, Object> response = new HashMap<>();
	    List<String> message = Util.verify(schema, tableName, column);
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
		        if (DbUtil.columnExists(con, tableName, column, schema)) {
		            if (DbUtil.executeQuery(con, schema, QueryGenerator.dropColumn(schema, tableName, column))) {
		                Util.addMessage(response, message, true, "Successfully dropped column.");
		            } else {
		                Util.addMessage(response, message, false, "Failed to drop column.");
		            }
		        } else {
		            Util.addMessage(response, message, false, "Column does not exist.");
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

	public static Map<String, Object> drop(String tableName, String column) { 
		return drop("", tableName, column);
	}
	
	public static Map<String, Object> alterType(String schema, String tableName, String column, String newDatatype, String oldDatatype) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verify(schema, tableName, column, newDatatype, oldDatatype);
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
				if (DbUtil.columnExists(con, tableName, column, schema)) {
					if (DbUtil.executeQuery(con, schema, QueryGenerator.alterType(schema, tableName, column, newDatatype))) {
						Util.addMessage(response, message, true, "Successfully changed column type.");
					} else {
						Util.addMessage(response, message, false, "Failed to change column type.");
					}
				} else {
					Util.addMessage(response, message, false, "Column does not exist.");
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

	public static Map<String, Object> alterType(String tableName, String column, String newDatatype, String oldDatatype) {
		return alterType("", tableName, column, newDatatype, oldDatatype);
	}
	
	public static Map<String, Object> alterSize(String schema, String tableName, String column, String datatype, String size) {
	    Map<String, Object> response = new HashMap<>();
	    List<String> message = Util.verifyInfo(schema, tableName, column, datatype, size);
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
		        if (DbUtil.columnExists(con, tableName, column, schema)) {
		            if (DbUtil.executeQuery(con, schema, QueryGenerator.alterSize(schema, tableName, column, datatype, size))) {
		                Util.addMessage(response, message, true, "Successfully changed column size.");
		            } else {
		                Util.addMessage(response, message, false, "Failed to change column size.");
		            }
		        } else {
		            Util.addMessage(response, message, false, "Column does not exist.");
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

	public static Map<String, Object> alterSize(String tableName, String column, String datatype, String size) {
		return alterSize("", tableName, column, datatype, size);
	}
	
	public static void main(String[] args) {
		Map<String, String> col1 = new HashMap<>();
		col1.put("name", "number");
		col1.put("type", "INT");
		col1.put("required", "true");
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
//		System.out.println(dropColumn("zero_code", "name_user", "brand"));
//		System.out.println(alterType("zc_core", "user_name", "id", "TINYINT", "INT"));
//		System.out.println(alterSize("zc_code_java", "wertyui", "name", "VARCHAR", "20"));
	}
}

