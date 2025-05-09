package de.zeroco.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
//import java.util.regex.Pattern;
import java.util.regex.Pattern;

/**
 * This class containing commonly used validations of input.
 * 
 * @author Tejaswini.S
 * @since 07-04-2025
 * @reviewed by
 */
public class Util {

	/**
	 * This a method named isBlank that takes an Object as a parameter. Check the
	 * input type and validate if it is blank.
	 * 
	 * @author Tejaswini.S
	 * @since 7-04-2025
	 * @param input
	 * @return true if input is empty
	 * @reviewed by
	 */
	@Deprecated
	public static boolean isBlankOld(Object input) {
		if (input == null)
			return true;
		if ((input instanceof Integer)) {
			if ((int) input <= 0)
				return true;
		} else if (input instanceof Long) {
			if ((long) input <= 0)
				return true;
		} else if (input instanceof Float) {
			if ((float) input <= 0.0f)
				return true;
		} else if (input instanceof Double) {
			if ((double) input <= 0.0)
				return true;
		} else if (input instanceof String) {
			if ((((String) input).trim().isEmpty()))
				return true;
		} else if ((input instanceof Character)) {
			if (Character.isWhitespace((Character) input))
				return true;
		} else if (input.getClass().isArray()) {
			if (java.lang.reflect.Array.getLength(input) == 0)
				return true;
		} else if (input instanceof Collection<?>) {
			if (((Collection<?>) input).isEmpty())
				return true;
		}
		return false;
	}

	public static boolean isBlank(Object input) {
		if (input == null) {
			return true;
		} else if (input.getClass().isArray()) {
			return java.lang.reflect.Array.getLength(input) == 0;
		} else if (input instanceof Collection<?>) {
			return ((Collection<?>) input).isEmpty();
		} else {
			return input.toString().trim().isEmpty();
		}
	}

	/**
	 * This is method named isValidEmail, takes parameter as String. It will
	 * validate email with regex.
	 * 
	 * @author Tejaswini.S
	 * @since 7-04-2025
	 * @param email
	 * @return true if input is valid
	 * @reviewed by
	 */
	public static boolean isValidEmail(String email) {
		String emailRegex = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$";
		return Pattern.matches(emailRegex, email);
	}

	/**
	 * This is a method named isPhoneNumber, takes parameter as String. It will
	 * validate Indian phone number with regex.
	 * 
	 * @author Tejaswini.S
	 * @since 7-04-2025
	 * @param phoneNumber
	 * @return true if input is valid
	 * @reviewed by
	 */
	public static boolean isValidPhone(String phone) {
		return phone.matches("^\\+?[1-9]\\d{1,14}$");
	}

	/**
	 * Validates the provided email and phone number.
	 * 
	 * @param email
	 * @param phone
	 * @return A list of error messages.
	 * @reviewed by
	 */
	public static List<String> verifyInfo(String email, String phone) {
		List<String> errors = new ArrayList<>();
		if (Util.isBlank(email)) {
			errors.add("Email is required.");
		} else if (!Util.isValidEmail(email)) {
			errors.add("Invalid email format.");
		}
		if (!Util.isBlank(phone) && !Util.isValidPhone(phone)) {
			errors.add("Invalid phone number.");
		}
		return errors;
	}

	public static List<String> verifyParamsOLd(LinkedHashMap<String, Object> params) {
		List<String> errors = new ArrayList<>();
		if (isBlank(params)) {
			errors.add("Please pass valid params");
		}
		return errors;
	}

	/**
	 * 
	 * @param params
	 * @param columns
	 * @return
	 */
	public static List<String> verifyParams(Map<String, Object> params, List<Map<String, Object>> columns) {
		List<String> errors = new ArrayList<>();
		for (Map<String, Object> col : columns) {
			Object value = params.get(col.get("name"));
			if (isBlank(value) && (Boolean)col.get("required") != null) {
				errors.add(col.get("name") + " is required");
			}
			if (value != null) {
				Integer maxLength = (Integer) col.get("maxLength");
				if (maxLength != null && maxLength < value.toString().length()) {
					errors.add(col.get("name") + "is greater than expected length");
				}
				Integer minLength = (Integer) col.get("minLength");
				if (minLength != null && minLength > value.toString().length()) {
					errors.add(col.get("name") + "is less than expected length");
				}
//				String pattern = (String) col.get("pattern");
//				if (pattern != null && !value.toString().matches(pattern)) {
//					errors.add(col.get("name") + "is not a valid pattern");
//				}
				
			} 
		}
		return errors;
	}

	public static String concateKeyValues(List<Map<String, Object>> input, String key) {
		String values = "";
		for (Map<String, Object> map : input) {
			Object value = map.get(key);
			if (value != null) {
				values += ", " + value.toString();
			}
		}
		return values.substring(1);
	}

	public static List<Object> values(List<Map<String, Object>> columns, Map<String, Object> params) {
		List<Object> values = new ArrayList<>();
		for (Map<String, Object> column : columns) {
			String name = (String) column.get("name");
			Object value = params.get(name);
			values.add(value);
		}
		return values;
	}
	
	public static List<String> columns(List<Map<String, Object>> columns) {
		List<String> columnValues = new ArrayList<>();
		for (Map<String, Object> column : columns) {
			columnValues.add((String) column.get("name"));
		}
		return columnValues;
	}
	
	public static List<String> verifyDetails(String schema, String tableName, List<Map<String, String>> columns) {
		List<String> errors = new ArrayList<>();
		if (schema == null) {
			errors.add("Schema is required.");
		}
		if (Util.isBlank(tableName)) {
			errors.add("Tablename is required.");
		}
		if (Util.isBlank(columns)) {
			errors.add("Column name is required");
		} else {
			for (Map<String, String> column : columns) {
				if (Util.isBlank(column.get("name"))) {
					errors.add("Column name is missing");
				} 
				if (Util.isBlank(column.get("type"))) {
					errors.add("Type is missing");
				} 
				if ("VARCHAR".equalsIgnoreCase(column.get("type")) || "CHAR".equalsIgnoreCase(column.get("type")) || "VARBINARY".equalsIgnoreCase(column.get("type")) || "BINARY".equalsIgnoreCase(column.get("type"))) {
					if (Util.isBlank(column.get("size"))) {
						errors.add("Size is missing");
					}
				} 
//				if (Util.isBlank(column.get("default"))) {
//					errors.add("Default value is missing");
//				} 
//				if (Util.isBlank(column.get("required"))) {
//					errors.add("Required is missing");
//				} 
//				if (Util.isBlank("unique")) {
//					errors.add("Unique value is missing");
//				}
			}
		}
		return errors;
	}
	
	public static List<String> verifyDetails(String schema, String tableName) {
		List<String> errors = new ArrayList<>();
		if (schema == null) {
			errors.add("Schema is required");
		}
		if (Util.isBlank(tableName)) {
			errors.add("Tablename is required.");
		} 
		return errors;
	}
	
	public static List<String> verifyTableNames(String schema,String newTableName, String oldTableName) {
		List<String> errors = new ArrayList<>();
		if (schema == null) {
			errors.add("Schema is required");
		}
		if (Util.isBlank(newTableName)) {
			errors.add("New table name is required to change");
		} 
		if (Util.isBlank(oldTableName)) {
			errors.add("Old table name is required to change");
		}
		return errors;
	}
	
	public static List<String> verifyDetails(String schema, String tableName, Map<String, String> column) {
		List<String> errors = new ArrayList<>();
		if (schema == null) {
			errors.add("Schema is required.");
		}
		if (Util.isBlank(tableName)) {
			errors.add("Tablename is required.");
		}
		if (Util.isBlank(column)) {
			errors.add("Column name is required");
		} else {
				if (Util.isBlank(column.get("name"))) {
					errors.add("Column name is missing");
				} 
				if (Util.isBlank(column.get("type"))) {
					errors.add("Type is missing");
				} 
				if ("VARCHAR".equalsIgnoreCase(column.get("type")) || "CHAR".equalsIgnoreCase(column.get("type")) || "BINARY".equalsIgnoreCase(column.get("type")) || "VARBINARY".equalsIgnoreCase(column.get("type")) || "BINARY".equalsIgnoreCase(column.get("type"))) {
					if (Util.isBlank(column.get("size"))) {
						errors.add("Size is missing");
					}
				} 
		}
		return errors;
	}
	
	public static List<String> verify(String schema, String tableName, String column) {
		List<String> errors = new ArrayList<>();
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(column)) errors.add("Column is required.");
		return errors;
	}
	
	public static List<String> verify(String schema, String tableName, String oldColumnName, String newColumnName) {
		List<String> errors = new ArrayList<>();
		if (oldColumnName.equals(newColumnName)) errors.add("Previous and present column names are same");
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(oldColumnName)) errors.add("Column is required.");
		if (Util.isBlank(newColumnName)) errors.add("New column name is required.");
		return errors;
	}
	
	public static List<String> verify(String schema, String tableName, String column, String newDataType, String oldDataType) {
		List<String> errors = new ArrayList<>();
		if (newDataType.equalsIgnoreCase(oldDataType)) errors.add("Column has same data type as you provided");
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(column)) errors.add("Column is required.");
		if (Util.isBlank(newDataType)) errors.add("Data type name is required.");
		if (Util.isBlank(oldDataType)) errors.add("Existing data type is required.");
		return errors;
	}
	
	public static List<String> verifyInfo(String schema, String tableName, String column, String datatype, String size) {
		List<String> errors = new ArrayList<>();
		if (!"VARCHAR".equalsIgnoreCase(datatype) && !"CHAR".equalsIgnoreCase(datatype) && !"BINARY".equalsIgnoreCase(datatype) && !"VARBINARY".equalsIgnoreCase(datatype)) errors.add("Size is not applicable for this data type");
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(column)) errors.add("Column is required.");
		if (Util.isBlank(datatype)) errors.add("Data type name is required.");
		if (Util.isBlank(size)) errors.add("Size is required");
		return errors;
	}
	
//	public static List<String> verification(String schema, String tableName, String column, String index) {
//		List<String> errors = new ArrayList<>();
//		if (schema == null) errors.add("Schema is required.");
//		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
//		if (Util.isBlank(column)) errors.add("Column is required.");
//		if (Util.isBlank(index)) errors.add("Index is required");
//		return errors;
//	}
	
	public static List<String> verification(String schema, String tableName, String index, String column) {
		List<String> errors = new ArrayList<>();
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(index)) errors.add("Index is required");
		if (Util.isBlank(column)) errors.add("Column is required");
		return errors;
	}
	
	public static void addMessage(Map<String, Object> response, List<String> messages, boolean success, String message) {
		 response.put("success", success);
        messages.add(message);
	}
	
	public static List<String> verifConstraintInfo(String schema, String tableName, String column, String constraintType, String constraintName, String constraintValue) {
		List<String> errors = new ArrayList<>();
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(column)) errors.add("Column is required.");
		if (constraintName == null) errors.add("Constraint name is required");
		if (constraintValue == null) errors.add("Constraint value is required");
		if (Util.isBlank(constraintType)) errors.add("Constraint type is required");
		return errors;
	}
	
	public static List<String> verifConstraintDetails(String schema, String tableName, String column, String constraintType, String constraintName, String datatype) {
		List<String> errors = new ArrayList<>();
		if (schema == null) errors.add("Schema is required.");
		if (Util.isBlank(tableName)) errors.add("Tablename is required.");
		if (Util.isBlank(column)) errors.add("Column is required.");
		if (constraintName == null) errors.add("Constraint name is required");
		if (Util.isBlank(datatype)) errors.add("Constraint type is required");
		return errors;
	}
}
