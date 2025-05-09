package de.zeroco.service;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.zeroco.util.*;

public class ConstraintMgmt {
	
	public static Map<String, Object> create(String schema, String tableName, String column, String constraintType, String constraintName, String constraintValue, String type) {
		Map<String, Object> response = new HashMap<>();
		List<String> message = Util.verifConstraintInfo(schema, tableName, column, constraintType, constraintName, constraintValue);
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
					if (!DbUtil.checkConstraint(con, schema, tableName, column, constraintType, constraintName, constraintValue)) {
						if (DbUtil.executeQuery(con, schema, QueryGenerator.getConstraintQuery(schema, tableName, column, constraintType, constraintName, constraintValue, type))) {
							Util.addMessage(response, message, true, "Constraint created on column successfully.");
						} else {
							Util.addMessage(response, message, false, "Failed to create constraint.");
						}
					} else {
						Util.addMessage(response, message, false, "Failed to add constraint.");
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
	
	public static Map<String, Object> create(String tableName, String column, String constraintType, String constraintName, String constraintValue, String type) {
		return create("", tableName, column, constraintType, constraintName, constraintValue, type);
	}
	
	public static Map<String, Object> drop(String schema, String tableName, String column, String constraintType, String constraintName, String datatype) {
	    Map<String, Object> response = new HashMap<>();
	    List<String> message = Util.verifConstraintDetails(schema, tableName, column, constraintType, constraintName, datatype);
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
	                if (DbUtil.checkConstraint(con, schema, tableName, column, constraintType, constraintName, null)) {
	                    String query = QueryGenerator.dropConstraint(schema, tableName, column, constraintType, constraintName, datatype);
	                    if (DbUtil.executeQuery(con, schema, query)) {
	                        Util.addMessage(response, message, true, "Constraint dropped successfully.");
	                    } else {
	                        Util.addMessage(response, message, false, "Failed to drop constraint.");
	                    }
	                } else {
	                    Util.addMessage(response, message, false, "Constraint does not exist.");
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
	
	public static Map<String, Object> drop(String tableName, String column, String constraintType, String constraintName, String datatype) {
		return drop(tableName, column, constraintType, constraintName, datatype);
	}

	public static void main(String[] args) {
//		System.out.println(create("zc_team", "chakri", "id", "NOT NULL", "pk_id", "", "INT"));
//		System.out.println(create("zc_team", "computer", "id", "PRIMARY KEY", "", "", "INT"));
//		System.out.println(create("zc_team", "computer", "id", "AUTO_INCREMENT", "", "", "INT"));
//		System.out.println(create("zc_team", "computer", "id", "UNIQUE", "uc_computer", "", "INT"));
//		System.out.println(drop("zc_team", "computer", "id", "NOT NULL", "", "INT"));
//		System.out.println(drop("zc_team", "computer", "id", "NOT NULL", "", "INT"));
//		System.out.println(drop("zc_team", "computer", "id", "AUTO_INCREMENT", "", "INT"));
//		System.out.println(drop("zc_team", "computer", "id", "UNIQUE", "uc_computer", "INT"));s
	}
}
