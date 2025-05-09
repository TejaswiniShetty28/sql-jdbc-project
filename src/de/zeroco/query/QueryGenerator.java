package de.zeroco.query;

import java.util.List;
import java.util.Map;
import de.zeroco.constants.Constant;

public class QueryGenerator {
	
	public static String createTable(String schema, String tableName, List<Map<String, String>> columns) {
		String concate = "";
		for (Map<String, String> column : columns) {
			concate +=  column.get("name") + " " + column.get("type");
//			String size = column.get("size");
			if ("VARCHAR".equalsIgnoreCase(column.get("type")) || "CHAR".equalsIgnoreCase(column.get("type")) || "BINARY".equalsIgnoreCase(column.get("type")) || "VARBINARY".equalsIgnoreCase(column.get("type"))) {
				    concate += "(" + column.get("size") + ")";
			}
			if (Boolean.parseBoolean(column.get("required"))) {
				concate += " NOT NULL";
			}
			if (!Util.isBlank(column.get("default_value"))) {
				concate += " DEFAULT " + column.get("default_value");
			}
			if (Boolean.parseBoolean(column.get("unique"))) {
				concate += " UNIQUE";
			}
			concate += ",\n";
		}
		return "CREATE TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " (\n" + concate.substring(0, concate.length() - 2) + "\n);";
	}
	
	public static String dropTable(String schema, String tableName) {
		return "DROP TABLE " + (Util.isBlank(schema) ? "" :  Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + ";";
	}
	
	public static String renameTable(String schema, String newTableName, String oldTableName) {
	    return "ALTER TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + oldTableName + Constant.GRAVE + " RENAME TO " + Constant.GRAVE + newTableName + Constant.GRAVE + ";";
	}

	public static String truncateTable(String schema, String tableName) {
	    return "TRUNCATE TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + ";";
	}
	
	public static String addColumn(String schema, String tableName, Map<String, String> column) {
		String concate = "";
		concate +=  column.get("name") + " " + column.get("type");
//		String size = column.get("size");
		if ("VARCHAR".equalsIgnoreCase(column.get("type")) || "CHAR".equalsIgnoreCase(column.get("type")) || "BINARY".equalsIgnoreCase(column.get("type")) || "VARBINARY".equalsIgnoreCase(column.get("type"))) {
			    concate += "(" + column.get("size") + ")";
		}
		if (Boolean.parseBoolean(column.get("required"))) {
			concate += " NOT NULL";
		}
		if (!Util.isBlank(column.get("default_value"))) {
			concate += " DEFAULT " + column.get("default_value");
		}
		if (Boolean.parseBoolean(column.get("unique"))) {
			concate += " UNIQUE";
		} 
		return "ALTER TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " ADD " + concate + ";";
	}
	
	public static String dropColumn(String schema, String tableName, String column) {
		return "ALTER TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " DROP COLUMN " + Constant.GRAVE + column + Constant.GRAVE + ";";
	}
	
	public static String alterType(String schema, String tableName, String column, String newDatatype) {
	    return "ALTER TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " MODIFY COLUMN " + Constant.GRAVE + column + Constant.GRAVE + " " + newDatatype + ";";
	}
	
	public static String alterSize(String schema, String tableName, String column, String datatype, String size) {
		return "ALTER TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " MODIFY " + Constant.GRAVE + column + Constant.GRAVE + datatype + "(" + size + ");" ;
	}
	
	public static String createIndex(String schema, String tableName, String column, String indexName) {
	    return "CREATE INDEX " + Constant.GRAVE + indexName + Constant.GRAVE + " ON " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " (" + Constant.GRAVE + column + Constant.GRAVE + ")";
	}
	
	public static String dropIndex(String schema, String tableName, String indexName) {
	    return "DROP INDEX IF EXISTS " + Constant.GRAVE + indexName + Constant.GRAVE + " ON " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") +  Constant.GRAVE + tableName + Constant.GRAVE;
	}
	
	public static String createUniqueIndex(String schema, String tableName, String column, String indexName) {
	    return "CREATE UNIQUE INDEX " + Constant.GRAVE + indexName + Constant.GRAVE + " ON " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " (" + Constant.GRAVE + column + Constant.GRAVE + ")";
	}
	
	//unique constraint
	public static String isColumnUnique(String schema, String tableName, String column) {
	    return "ALTER TABLE " + (Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".") + Constant.GRAVE + tableName + Constant.GRAVE + " ADD UNIQUE (" + Constant.GRAVE + column + Constant.GRAVE + ");";
	}
	
	public static String getConstraintQuery(String schema, String tableName, String columnName, String constraintType, String constraintName, String constraintValue, String datatype) {
	    String escapedSchema = Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".";
	    String escapedTable = Constant.GRAVE + tableName + Constant.GRAVE;
	    String escapedColumn = Constant.GRAVE + columnName + Constant.GRAVE;
	    String escapedConstraint = Constant.GRAVE + constraintName + Constant.GRAVE;
	    switch (constraintType.toUpperCase()) {
	        case "UNIQUE":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " ADD CONSTRAINT " + escapedConstraint + " UNIQUE (" + escapedColumn + ")";
	        case "PRIMARY KEY":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " ADD CONSTRAINT " + escapedConstraint + " PRIMARY KEY (" + escapedColumn + ")";
	        case "AUTO_INCREMENT":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " MODIFY " + escapedColumn + " " + datatype + " AUTO_INCREMENT";
	        case "NOT NULL":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " MODIFY " + escapedColumn + " " + datatype + " NOT NULL";
	        default:
	            return "";
	    }
	}

	public static String dropConstraint(String schema, String tableName, String columnName, String constraintType, String constraintName, String datatype) {
	    String escapedSchema = Util.isBlank(schema) ? "" : Constant.GRAVE + schema + Constant.GRAVE + ".";
	    String escapedTable = Constant.GRAVE + tableName + Constant.GRAVE;
	    String escapedColumn = Constant.GRAVE + columnName + Constant.GRAVE;
	    String escapedConstraint = Constant.GRAVE + constraintName + Constant.GRAVE;
	    switch (constraintType.toUpperCase()) {
	        case "UNIQUE":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " DROP INDEX " + escapedConstraint;
	        case "PRIMARY KEY":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " DROP PRIMARY KEY";
	        case "NOT NULL":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " MODIFY " + escapedColumn + " " + datatype;
	        case "AUTO_INCREMENT":
	            return "ALTER TABLE " + escapedSchema + escapedTable + " MODIFY " + escapedColumn + " " + datatype;
	        default:
	            return "";
	    }
	}

	public static void main(String[] args) {
		System.out.println(getConstraintQuery("zc_team", "chakarm", "name", "NOT NULL", "", "", "VARCHAR"));
//		List<Map<String, String>> params = new ArrayList<>();
//

//		col1.put("name", "tinyint");
//		col1.put("type", "TINYINT");
//		col1.put("required", "false");
//
//		Map<String, String> col2 = new HashMap<>();
//		col2.put("name", "smallint");
//		col2.put("type", "SMALLINT");
//		col2.put("required", "false");
//
//		Map<String, String> col3 = new HashMap<>();
//		col3.put("name", "mediumint");
//		col3.put("type", "MEDIUMINT");
//		col3.put("required", "false");
//
//		Map<String, String> col4 = new HashMap<>();
//		col4.put("name", "int");
//		col4.put("type", "INT");
//		col4.put("required", "false");
//
//		Map<String, String> col5 = new HashMap<>();
//		col5.put("name", "bigint");
//		col5.put("type", "BIGINT");
//		col5.put("required", "false");
//
//		Map<String, String> col6 = new HashMap<>();
//		col6.put("name", "decimal");
//		col6.put("type", "DECIMAL(10,2)");
//		col6.put("required", "false");
//
//		Map<String, String> col7 = new HashMap<>();
//		col7.put("name", "numeric");
//		col7.put("type", "NUMERIC(10,2)");
//		col7.put("required", "false");
//
//		Map<String, String> col8 = new HashMap<>();
//		col8.put("name", "float");
//		col8.put("type", "FLOAT");
//		col8.put("required", "false");
//
//		Map<String, String> col9 = new HashMap<>();
//		col9.put("name", "double");
//		col9.put("type", "DOUBLE");
//		col9.put("required", "false");
//
//		Map<String, String> col10 = new HashMap<>();
//		col10.put("name", "bit");
//		col10.put("type", "BIT(1)");
//		col10.put("required", "false");
//
//		Map<String, String> col11 = new HashMap<>();
//		col11.put("name", "col_boolean");
//		col11.put("type", "BOOLEAN");
//		col11.put("required", "false");
//
//		Map<String, String> col12 = new HashMap<>();
//		col12.put("name", "char");
//		col12.put("type", "CHAR");
//		col12.put("size", "10");
//		col12.put("required", "false");
//
//		Map<String, String> col13 = new HashMap<>();
//		col13.put("name", "varchar");
//		col13.put("type", "VARCHAR");
//		col13.put("size", "100");
//		col13.put("required", "false");
//
//		Map<String, String> col14 = new HashMap<>();
//		col14.put("name", "text");
//		col14.put("type", "TEXT");
//		col14.put("required", "false");
//
//		Map<String, String> col15 = new HashMap<>();
//		col15.put("name", "tinytext");
//		col15.put("type", "TINYTEXT");
//		col15.put("required", "false");
//
//		Map<String, String> col16 = new HashMap<>();
//		col16.put("name", "mediumtext");
//		col16.put("type", "MEDIUMTEXT");
//		col16.put("required", "false");
//
//		Map<String, String> col17 = new HashMap<>();
//		col17.put("name", "longtext");
//		col17.put("type", "LONGTEXT");
//		col17.put("required", "false");
//
//		Map<String, String> col18 = new HashMap<>();
//		col18.put("name", "enum");
//		col18.put("type", "ENUM('small', 'medium', 'large')");
//		col18.put("required", "false");
//
//		Map<String, String> col19 = new HashMap<>();
//		col19.put("name", "set");
//		col19.put("type", "SET('a','b','c')");
//		col19.put("required", "false");
//
//		Map<String, String> col20 = new HashMap<>();
//		col20.put("name", "binary");
//		col20.put("type", "BINARY");
//		col20.put("size", "16");
//		col20.put("required", "false");
//
//		Map<String, String> col21 = new HashMap<>();
//		col21.put("name", "varbinary");
//		col21.put("type", "VARBINARY");
//		col21.put("size", "32");
//		col21.put("required", "false");
//
//		Map<String, String> col22 = new HashMap<>();
//		col22.put("name", "tinyblob");
//		col22.put("type", "TINYBLOB");
//		col22.put("required", "false");
//
//		Map<String, String> col23 = new HashMap<>();
//		col23.put("name", "blob");
//		col23.put("type", "BLOB");
//		col23.put("required", "false");
//
//		Map<String, String> col24 = new HashMap<>();
//		col24.put("name", "mediumblob");
//		col24.put("type", "MEDIUMBLOB");
//		col24.put("required", "false");
//
//		Map<String, String> col25 = new HashMap<>();
//		col25.put("name", "longblob");
//		col25.put("type", "LONGBLOB");
//		col25.put("required", "false");
//
//		Map<String, String> col26 = new HashMap<>();
//		col26.put("name", "date");
//		col26.put("type", "DATE");
//		col26.put("required", "false");
//
//		Map<String, String> col27 = new HashMap<>();
//		col27.put("name", "datetime");
//		col27.put("type", "DATETIME");
//		col27.put("required", "false");
//
//		Map<String, String> col28 = new HashMap<>();
//		col28.put("name", "timestamp");
//		col28.put("type", "TIMESTAMP");
//		col28.put("required", "false");
//
//		Map<String, String> col29 = new HashMap<>();
//		col29.put("name", "time");
//		col29.put("type", "TIME");
//		col29.put("required", "false");
//
//		Map<String, String> col30 = new HashMap<>();
//		col30.put("name", "year");
//		col30.put("type", "YEAR");
//		col30.put("required", "false");
//
//		// Add all to params
//		params.add(col1);
//		params.add(col2);
//		params.add(col3);
//		params.add(col4);
//		params.add(col5);
//		params.add(col6);
//		params.add(col7);
//		params.add(col8);
//		params.add(col9);
//		params.add(col10);
//		params.add(col11);
//		params.add(col12);
//		params.add(col13);
//		params.add(col14);
//		params.add(col15);
//		params.add(col16);
//		params.add(col17);
//		params.add(col18);
//		params.add(col19);
//		params.add(col20);
//		params.add(col21);
//		params.add(col22);
//		params.add(col23);
//		params.add(col24);
//		params.add(col25);
//		params.add(col
//		params.add(col27);
//		params.add(col28);
//		params.add(col29);
//		params.add(col30);
//        System.out.println(createTable("check", params));
	}
}
