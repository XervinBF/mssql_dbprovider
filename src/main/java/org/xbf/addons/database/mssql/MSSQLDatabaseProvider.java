package org.xbf.addons.database.mssql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.LoggerFactory;
import org.xbf.core.Config.XDBConfig;
import org.xbf.core.Data.Connector.DBResult;
import org.xbf.core.Data.Connector.DbType;
import org.xbf.core.Data.Connector.IDBProvider;
import org.xbf.core.Utils.Map.FastMap;

import ch.qos.logback.classic.Logger;

@DbType(name = "MSSQL")
public class MSSQLDatabaseProvider implements IDBProvider {

	static final Logger logger = (Logger) LoggerFactory.getLogger(MSSQLDatabaseProvider.class);
	
    Connection con = null;
	
	public void openConnection(XDBConfig config) {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			logger.error("MSSQL Driver not found", e);
		}
        try {
			con = DriverManager.getConnection("jdbc:sqlserver://" + config.connectionString);
		} catch (SQLException e) {
			con = null;
			e.printStackTrace();
		}
		
	}

	public void createTable(String tableName, HashMap<String, String> fields) {
		char separator = '"';
		String sql = "CREATE TABLE " + separator + tableName + separator + " (";
		for (String fieldName : fields.keySet()) {
			String type = fields.get(fieldName);
			String query = separator + fieldName + separator + " " + type + " NULL";
			if (!type.equalsIgnoreCase("int")) {
				query += " DEFAULT (NULL)";
			}
			sql += query + ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")";
		logger.warn("Creating table " + tableName + ": " + sql);
		update(sql);
	}

	public void addFieldToTable(String table, String fieldName, String fieldType) {
		String query = "ALTER TABLE " + table + " ADD \"" + fieldName + "\" " + fieldType + " NULL";
		if (!fieldType.equalsIgnoreCase("int")) {
			query += " DEFAULT (NULL)";
		}
		update(query);
	}

	public String getFieldType(String javaFieldClassName) {
		switch (javaFieldClassName) {
		case "int":
			return "INT";

		case "String":
			return "VARCHAR(" + getMaxValueForField("VARCHAR") + ")";
			
		case "boolean":
			return "VARCHAR(5)"; // true - 4 characters, false - 5 characters

		default:
			return "VARCHAR(" + getMaxValueForField("VARCHAR") + ")"; // Allow pretty much any datatype
		}
	}

	public String getMaxValueForField(String fieldType) {
		switch (fieldType.toUpperCase()) {
		case "VARCHAR":
			return "MAX";

		default:
			return "128";
		}
	}

	public HashMap<String, String> getFields(String table) {
		FastMap<String, String> map = new FastMap<String, String>();
		String query = "SELECT \r\n" + "    c.name 'Column Name',\r\n" + "    t.Name 'Data type',\r\n"
				+ "    c.max_length 'Max Length',\r\n" + "    c.precision ,\r\n" + "    c.scale ,\r\n"
				+ "    c.is_nullable,\r\n" + "    ISNULL(i.is_primary_key, 0) 'Primary Key'\r\n" + "FROM    \r\n"
				+ "    sys.columns c\r\n" + "INNER JOIN \r\n"
				+ "    sys.types t ON c.user_type_id = t.user_type_id\r\n" + "LEFT OUTER JOIN \r\n"
				+ "    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id\r\n"
				+ "LEFT OUTER JOIN \r\n"
				+ "    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id\r\n" + "WHERE\r\n"
				+ "    c.object_id = OBJECT_ID('" + table + "')";
		try {
			ResultSet r = query(query);
			while (r.next()) {
				map.add(r.getString(1), r.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return map.getMap();
	}

	public DBResult getData(String table) throws Exception {
		return DBResult.fromResultSet(query("SELECT * FROM " + table));
	}

	public DBResult getData(String table, ArrayList<String> fieldsToGet) throws Exception {
		return DBResult.fromResultSet(query("SELECT " + String.join(", ", fieldsToGet) + " FROM " + table));
	}

	public DBResult getData(String table, HashMap<String, String> where) throws Exception {
		return DBResult.fromResultSet(query("SELECT * FROM " + table + " WHERE " + buildWhere(where)));

	}

	public DBResult getData(String table, ArrayList<String> fieldsToGet, HashMap<String, String> where) throws Exception {
		return DBResult.fromResultSet(query("SELECT " + String.join(", ", fieldsToGet) + " FROM " + table + " WHERE " + buildWhere(where)));
	}

	public DBResult getData(String table, HashMap<String, String> where, int limit) throws Exception {
		return DBResult.fromResultSet(query("SELECT TOP(" + limit + ") * FROM " + table + " WHERE " + buildWhere(where)));

	}

	public DBResult getData(String table, ArrayList<String> fieldsToGet, HashMap<String, String> where, int limit) throws Exception {
		return DBResult.fromResultSet(query("SELECT TOP(" + limit + ") " + String.join(", ", fieldsToGet) + " FROM " + table + " WHERE " + buildWhere(where)));
	}

	public int getMax(String table, String field) {
		try {
			ResultSet s = query("SELECT MAX(" + field + ") FROM " + table);
			s.next();
			return s.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		}
	}

	public boolean has(String table, HashMap<String, String> where) {
		try {
			ResultSet r = query("SELECT count(*) FROM " + table + " WHERE " + buildWhere(where) + ";");
			r.next();
			int re = r.getInt(1);
			return re != 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void insertData(String table, HashMap<String, String> set) {
		ArrayList<String> vals = new ArrayList<String>();
		ArrayList<String> data = new ArrayList<String>();
		for (String f : set.keySet()) {
			vals.add(f);
			data.add(set.get(f));
		}
		update("INSERT INTO " + table + " (" + String.join(",", vals) + ") VALUES ('" + String.join("','", data) + "');");
	}

	public void updateData(String table, HashMap<String, String> set, HashMap<String, String> where) {
		String vals = "";
		for (String key : set.keySet()) {
			vals += key + "='" + set.get(key) + "',";
		}
		vals = vals.substring(0, vals.length() - 1);
		
		update("UPDATE " + table + " SET " + vals + " WHERE " + buildWhere(where));
	}

	public void delete(String table, HashMap<String, String> where) {
		update("DELETE FROM " + table + " WHERE " + buildWhere(where));
	}
	
	

	public boolean shouldCreateTable(String exceptionMessage) {
		return exceptionMessage.startsWith("Invalid object name");
	}

	public boolean shouldAddField(String exceptionMessage) {
		return exceptionMessage.startsWith("The column name ") && exceptionMessage.endsWith(" is not valid.");
	}
	
	String buildWhere(HashMap<String, String> where) {
		String str = "";
		for (String key : where.keySet()) {
			str += key + "='" + where.get(key) + "'";
		}
		return str;
	}
	
	
	
	ResultSet query(String query) throws SQLException {
		try {
			PreparedStatement ps = con.prepareStatement(query);
			ResultSet set = ps.executeQuery();
			return set;
		} catch (SQLException e) {
			logger.error("SQL Exception on query: " + query);
			e.printStackTrace();
			throw e;
		}
	}
	
	 void update(String query) {
        PreparedStatement ps;
		try {
			ps = con.prepareStatement(query);
	        ps.executeUpdate();
		} catch (Exception e) {
			logger.error("SQL Exception on query: " + query);
			e.printStackTrace();
		}
	}
	
	
}
