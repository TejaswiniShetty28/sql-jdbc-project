package de.zeroco.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.zeroco.constant.Constant;

/**
 * This class is for managing database connections using JDBC. 
 * 
 * @author Tejaswini.S
 * @since 07-04-2025
 * @reviewed by
 */
public class DbUtil {
	
//	public static final List<Map<String, Connection>> CONNECTION = new ArrayList<>();
//
//	static {
//	    Connection connection = DbUtil.getConnection();
//	    Map<String, Connection> map = new HashMap<>();
//	    map.put("default", connection);
//	    CONNECTION.add(map);
//	}
	
	/**
	 * This method establishes and returns a database connection using JDBC.
	 * 
	 * @author Tejaswini.S
	 * @since 07-04-2025
	 * @return Connection object if successful, otherwise null
	 * @reviewed by 
	 */
//	public static Connection getConnection(String schema) {
//		String url = "";
//		Connection connect = null;
//		try {
//			Class.forName(Constant.REGISTER_DRIVER);
//			if (!Util.isBlank(schema)) {
//				url = "jdbc:mysql://localhost:3306/" + schema;
//			} else {
//				url = Constant.DB_URL;
//			}
////			System.out.println("Connection :" + getConnectedSchema(connect));
//			connect = DriverManager.getConnection(url, Constant.DB_USER, Constant.DB_PASSWORD); 
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return connect;
//	}
	
	public static Connection getConnection(String schema) {
	    String url;
	    Connection connect = null;
	    try {
	        Class.forName(Constant.REGISTER_DRIVER);
	        if (!Util.isBlank(schema)) {
	            url = "jdbc:mysql://localhost:3306/" + schema;
	        } else {
	        	url = Constant.DB_URL;
	        }
	        connect = DriverManager.getConnection(url, Constant.DB_USER, Constant.DB_PASSWORD); 
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return connect;
	}
	
	public static Connection getConnection() {
		return getConnection("");
	}
	
	public static Connection getConnectionToSchema(String schema) throws SQLException {
	    String url = "jdbc:mysql://localhost:3306/" + schema; 
	    String username = "root";  
	    String password = "the@123";  
	    Connection con = null;
	    try {
	        con = DriverManager.getConnection(url, username, password);
	    } catch (SQLException e) {
	    	e.printStackTrace();
	    }
	    return con;
	}

	/**
	 * This method safely closes the given database connection.
	 * 
	 * @author Tejaswini.S
	 * @since 07-04-2025
	 * @param connection 
	 * @return true if connection is closed, false if connection is not closed.
	 * @reviewed by 
	 */
//	public static boolean closeConnection(Connection connection) {
//		try {
//			if (!connection.isClosed()) {
//				connection.close();
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return true;
//	}
	
	public static int getId(String schema, String tableName, String conditionColumn, Object value) {
	    Connection connection = null;
	    int id = 0;
	    try {
	        connection = getConnection(schema); 
	        String query = "SELECT id FROM " + schema + "." + tableName + " WHERE " + conditionColumn + " = ?";
	        PreparedStatement ps = connection.prepareStatement(query);
	        ps.setObject(1, value);  
	        ResultSet rs = ps.executeQuery();
	        if (rs.next()) {
	            id = rs.getInt("id");
	        }
	        rs.close();
	        ps.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	    } finally {
	        closeConnection(connection);
	    }
	    return id; 
	}

	/**
	 * this method is used to insert the data into table and get the pk_id of value
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
	public static int getGeneratedKey(String schema, String tableName, List<String> columns, List<Object> value) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(columns) && Util.isBlank(value)) return 0;
		PreparedStatement statement;
		int rowId = 0;
		try {
			statement = getConnection(schema).prepareStatement(QueryBuilder.getInsertQuery(schema, tableName, columns), Statement.RETURN_GENERATED_KEYS);
			for (int i = 1; i <= columns.size(); i++) {
				statement.setObject(i, value.get(i - 1));
			}
			statement.executeUpdate();
			ResultSet set = statement.getGeneratedKeys();
			if (set.next()) {
				rowId = set.getInt(1);
			}
			statement.close();
			set.close();
			closeConnection(getConnection(schema));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rowId;
	}
	
	/**
	 * this method is used to delete the data into table and get the pk_id of values
	 * 
	 * @author Tejaswini.S
	 * @since 08-04-2025
	 * @param connection
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param value
	 * @return int
	 * @reviewed by
	 */
	public static int delete(String schema, String tableName, String conditionColumn, Object value) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && (Util.isBlank(conditionColumn) && (Util.isBlank(value)))) return 0;
		int rowsDeleted = 0;
		try {
			PreparedStatement statement = getConnection(schema).prepareStatement(QueryBuilder.getDeleteQuery(schema, tableName, conditionColumn));
			statement.setObject(1, value);
			rowsDeleted = statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rowsDeleted;
	}
	
	/**
	 * this method is used to get a data from a row by select query
	 * 
	 * @author Tejaswini.S
	 * @since 08-04-2025
	 * @param connection
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param value
	 * @return int
	 * @reviewed by
	 */
	public static String getQuery(String schema, String tableName, List<String> columns, String conditionColumn, String values) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(columns)) return null;
		String query = QueryBuilder.getListQuery(schema, tableName, columns);
		return query.substring(0, query.length() - 1) + " WHERE " + conditionColumn + " IN (" + values + ") ;";
	}
	
	public static List<Map<String, Object>> list(String schema, String tableName, List<String> columns) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && Util.isBlank(columns)) return null;
		List<Map<String, Object>> listOfMaps = new ArrayList<>();
		String columnName = "";
		Object columnValue = "";
		int countColumns = 0;
		Connection connection = null;
		try {
			connection = getConnection(schema);
			PreparedStatement statement = connection.prepareStatement(QueryBuilder.getListQuery(schema, tableName, columns));
			ResultSet set = statement.executeQuery();
			ResultSetMetaData metaData = set.getMetaData();
			countColumns = metaData.getColumnCount();
			while (set.next()) {
				Map<String, Object> map = new HashMap<>();
				for (int i = 1; i <= countColumns; i++) {
					columnName = metaData.getColumnName(i);
					columnValue = set.getObject(i);
					map.put(columnName, columnValue);
				}
				listOfMaps.add(map);
			}
			statement.close();
			set.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConnection(connection);
		}
		return listOfMaps;
	}
	
	public static int update(Connection connection, String schema, String tableName, List<String> columns, List<Object> values, String conditionColumn, Object value) {
		if ((Util.isBlank(schema) && Util.isBlank(tableName)) && (Util.isBlank(columns)) && (Util.isBlank(conditionColumn)) && (Util.isBlank(values)) && (Util.isBlank(value))) return 0;
		connection = getConnection(schema);
		int effectedRows = 0;
		try {
			String query = QueryBuilder.getUpdateQuery(schema, tableName, columns, conditionColumn, value);
			PreparedStatement statement = connection.prepareStatement(query);
			int i = 1;
				for (Object key : values) {
					statement.setObject(i , key);
					i++;
				}
			effectedRows = statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConnection(connection);
		}
		return effectedRows;
	}
	
	public static boolean executeQuery(Connection con, String schema, String sql) {
	    Statement stmt = null;
	    try {
	        stmt = con.createStatement();
	        stmt.execute(sql);
	        return true;
	    } catch (SQLException e) {
	        e.printStackTrace();
	        return false;
	    } finally {
	        try {
	            if (stmt != null) stmt.close(); 
	        } catch (SQLException e) {
	            e.printStackTrace(); 
	        }
	    }
	}

	public static boolean isTableExists(Connection con, String schema, String tableName) {
	    ResultSet rs = null;
	    try {
	        DatabaseMetaData metaData = con.getMetaData();
	        if (Util.isBlank(schema)) schema = con.getCatalog();
	        rs = metaData.getTables(schema, null, tableName, new String[] { "TABLE" });
	        return rs.next();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    } finally {
	    	closeResultSet(rs);
	    }
	    return false;
	}

//	public static boolean columnExists(Connection conn, String tableName, String columnName, String schema) throws SQLException {
//	    DatabaseMetaData metaData = conn.getMetaData();
//	    if (Util.isBlank(schema)) schema = conn.getCatalog();
//	    try (ResultSet rs = metaData.getColumns(schema, null, tableName, columnName)) {
//	        return rs.next(); 
//	    }
//	}
	
	public static boolean columnExists(Connection conn, String tableName, String columnName, String schema) {
	    ResultSet rs = null;
	    try {
	        DatabaseMetaData metaData = conn.getMetaData();
	        if (Util.isBlank(schema)) schema = conn.getCatalog();
	        rs = metaData.getColumns(schema, null, tableName, columnName);
	        return rs.next();
	    } catch (SQLException e) {
	        e.printStackTrace(); 
	    } finally {
	    	closeResultSet(rs);
	    }
	    return false;
	}

	public static boolean isIndexExists(Connection conn, String schema, String tableName, String indexName) {
	    ResultSet rs = null;
	    try {
	        DatabaseMetaData metaData = conn.getMetaData();
	        rs = metaData.getIndexInfo(null, schema, tableName, false, false);
	        while (rs.next()) {
	            String existingIndex = rs.getString("INDEX_NAME");
	            if (indexName.equalsIgnoreCase(existingIndex)) {
	                return true;
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	    } finally {
	    	closeResultSet(rs);
	    }
	    return false;
	}

	
//	public static boolean schemaExists(String schemaName) {
//	    try (PreparedStatement stmt = DbUtil.getConnection(schemaName).prepareStatement("SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?")) {
//	        stmt.setString(1, schemaName);
//	        try (ResultSet rs = stmt.executeQuery()) {
//	            if (rs.next()) {
//	                return true;
//	            }
//	        }
//	    } catch (SQLException e) {
////	        e.printStackTrace();
//	    }
//	    return false;
//	}
	
	public static boolean schemaExists(Connection con, String schemaName) {
	    PreparedStatement stmt = null;
	    ResultSet rs = null;
	    try {
	        stmt = con.prepareStatement("SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?");
	        stmt.setString(1, schemaName);
	        rs = stmt.executeQuery();
	        if (rs.next()) {
	            return true;
	        }
	    } catch (SQLException e) {
	        e.printStackTrace();
	    } finally {
	    	closeResultSet(rs);
	        try {
	            if (stmt != null) stmt.close();
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }
	    return false;
	}
	
	public static String getConnectedSchema(Connection connection) {
	    try {
	        return connection.getCatalog(); 
	    } catch (SQLException e) {
	        e.printStackTrace();
	        return null;
	    }
	}
	
	public static void createSchema(Connection con, String schemaName) {
	    Statement stmt = null;
	    try {
	        stmt = con.createStatement();
	        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + schemaName);
	    } catch (SQLException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            if (stmt != null) stmt.close();
	        } catch (SQLException e) {
	            e.printStackTrace(); 
	        }
	    }
	}
	
	private static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean isUnique(Connection conn, String schema, String tableName, String columnName, String constraintName) {
        ResultSet rs = null;
        try {
            rs = conn.getMetaData().getIndexInfo(null, schema, tableName, true, false);
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String colName = rs.getString("COLUMN_NAME");
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                if (!nonUnique && columnName.equalsIgnoreCase(colName)) {
                    if (constraintName == null || constraintName.equalsIgnoreCase(indexName)) {
                        return true;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(rs);
        }
        return false;
    }

    public static boolean isPrimaryKeyExists(Connection conn, String schema, String tableName, String columnName) {
        ResultSet rs = null;
        try {
            rs = conn.getMetaData().getPrimaryKeys(null, schema, tableName);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                if (columnName.equalsIgnoreCase(colName)) {
                    return true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(rs);
        }
        return false;
    }

    public static boolean isAutoIncrement(Connection conn, String schema, String tableName, String column, String constraint) {
        ResultSet rs = null;
        try {
            rs = conn.getMetaData().getColumns(null, schema, tableName, column);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                String isAuto = rs.getString("IS_AUTOINCREMENT");
                if (column.equalsIgnoreCase(colName)) {
                    if (constraint == null || constraint.equalsIgnoreCase(isAuto)) {
                        return "YES".equalsIgnoreCase(isAuto);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(rs);
        }
        return false;
    }

    public static boolean isColumnNotNullable(Connection conn, String schema, String tableName, String columnName) {
        ResultSet rs = null;
        try {
            rs = conn.getMetaData().getColumns(null, schema, tableName, columnName);
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                int nullable = rs.getInt("NULLABLE");
                if (columnName.equalsIgnoreCase(col)) {
                    return nullable == DatabaseMetaData.columnNoNulls;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeResultSet(rs);
        }
        return false;
    }

    public static boolean checkConstraint(Connection conn, String schema, String tableName, String columnName, String constraintType, String constraintName, String constraintValue) {
        switch (constraintType.toUpperCase()) {
            case "UNIQUE":
                return isUnique(conn, schema, tableName, columnName, constraintName);
            case "PRIMARY KEY":
                return isPrimaryKeyExists(conn, schema, tableName, columnName);
            case "AUTO_INCREMENT":
                return isAutoIncrement(conn, schema, tableName, columnName, constraintName);
            case "NOT NULL":
                return isColumnNotNullable(conn, schema, tableName, columnName);
            default:
                return false;
        }
    }
    
    public static void closeConnection(Connection con) {
        try {
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void closeStatement(Statement stmt) {
    	try {
    		if (stmt != null) stmt.close();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
    public static List<String> list(Connection con, String query) {
    	List<String> emails = new ArrayList<String>();
    	ResultSet rs = null;
    	Statement stmt = null;
    	try {
    		stmt = con.createStatement();
    		rs = stmt.executeQuery(query);
    		while (rs.next()) {
    			emails.add(rs.getString("email"));
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		closeResultSet(rs);
    		closeConnection(con);
    		closeStatement(stmt);
    	}
    	return emails;
    }
}
