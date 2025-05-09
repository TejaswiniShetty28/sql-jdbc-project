package de.zeroco.query;

import java.util.List;
import de.zeroco.constant.Constant;

/**
 * Provides method to construct standard SQL query such as INSERT. 
 *
 * @author Tejaswini.S
 * @since 07-04-2025
 * @reviewed by 
 */
public class QueryBuilder {
	
	/**
	 * Generates a sql INSERT query string for the specified schema, table, and columns.
	 *
	 * @author Tejaswini.S
	 * @since 07-04-2025
	 * @param schema    
	 * @param tableName  
	 * @param columns   
	 * @return a SQL INSERT query 
	 * @reviewed by
	 */
	public static String getInsertQuery(String schema, String tableName, List<String> columns) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(columns)) return null;
		String columnNames = "";
		String values = "";
		for (String column : columns) {
			columnNames += "," + Constant.GRAVE + column + Constant.GRAVE;
			values += ", ?";
		}
		return "INSERT INTO " + Constant.GRAVE + schema + Constant.GRAVE  + "." + Constant.GRAVE  + tableName + Constant.GRAVE  + " (" + columnNames.substring(1) + ") VALUES (" + values.substring(1) + ") ;";
	}
	
	/**
     * Generates a SELECT query which counts:
     *
     * @author Tejaswini
     * @since 07-04-2025
     * @param schema 
     * @param table t
     * @param 
     * @return the constructed SQL query string
     * @reviewed by
     */
	public static String getCountQuery(String schema, String table, String column) {
        return "SELECT COUNT(*) FROM " + schema + "." + table + " WHERE " + column + " = ?";
    }
	
	/**
     * Generates a delete query for given schema, table and condition
     *
     * @author Tejaswini
     * @since 07-04-2025
     * @param schema 
     * @param table t
     * @param 
     * @return the constructed SQL query string
     * @reviewed by
     */
	public static String getDeleteQuery(String schema, String tableName, String conditionColumn) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(conditionColumn)) return null;
		return "DELETE FROM " + Constant.GRAVE + schema + Constant.GRAVE + "." + Constant.GRAVE + tableName + Constant.GRAVE + " WHERE " + conditionColumn + "  = ? ;";
	}
	
	/**
	 * this method is used to generate query for get the row data
	 * 
	 * @author Tejaswini.S
	 * @since 07-04-2025
	 * @param connection
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param value
	 * @return int
	 * @reviewed by
	 */
	public static String getListQuery(String schema, String tableName, List<String> columns) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(columns)) return null;
		String columnName = "";
		for (String column : columns) {
			columnName += "," + Constant.GRAVE + column + Constant.GRAVE;
		}
		return "SELECT " + (columns.isEmpty() ? "*" : columnName.substring(1)) + " FROM " + Constant.GRAVE + schema + Constant.GRAVE + "." + Constant.GRAVE + tableName + Constant.GRAVE + ";";
	}
	
	/**
	 * this method is used to generate update query
	 * 
	 * @author Tejaswini.S
	 * @since 07-04-2025
	 * @param connection
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param value
	 * @return int
	 * @reviewed by
	 */
	public static String getUpdateQuery(String schema, String tableName, List<String> columns, String conditionColumn,Object value) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(columns) && Util.isBlank(conditionColumn)) return null;
		String columnNames = "";
		for (String column : columns) {
			columnNames += "," + Constant.GRAVE + column + Constant.GRAVE + " = ?";
		}
		if(value instanceof String) {
			value = ",\""+value + "\"";
			value= ((String) value).substring(1);
		}
		return "UPDATE " + Constant.GRAVE + schema + Constant.GRAVE + "." + Constant.GRAVE + tableName + Constant.GRAVE + " SET " + columnNames.substring(1) + " WHERE " + conditionColumn + " = "+value+";";
	}
}

