package com.mr.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Mysql {
    private Connection _conn;

    public Mysql() {
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public void connect(String url, String dbuser, String dbpass) throws SQLException {
        this.connect(url, dbuser, dbpass, "");
    }

    public void connect(String url, String dbuser, String dbpass, String dbname) throws SQLException {
        this.close();
        if (url.startsWith("mysql://")) {
            url = "jdbc:" + url;
        } else {
            url = "jdbc:mysql://" + url+":3306";
        }
        if (!"".equals(dbname)) {
            if (url.endsWith("/")) {
                url += dbname;
            } else {
                url += "/" + dbname;
            }
        }
        url = url + "?useUnicode=true&characterEncoding=utf-8";
        _conn = DriverManager.getConnection(url, dbuser, dbpass);
    }

    public void close() throws SQLException {
        if (null != _conn && !_conn.isClosed()) {
            _conn.close();
        }
    }

    public boolean query(String sql) throws SQLException {
        try {
            Statement statement = _conn.createStatement();
            statement.execute(sql);
            return true;
        } catch (Exception e) {
            System.err.println(e.toString());
            return false;
        }
    }

    private ResultSet _query(String sql) throws SQLException {
        Statement statement = _conn.createStatement();
        return statement.executeQuery(sql);
    }

    public String escape(String string) {
        return string;
    }

    private HashMap<String, String> _fetchAssoc(ResultSet rs) throws SQLException {
        if (rs.next()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            if (columnCount > 0) {
                HashMap<String, String> row = new HashMap<String, String>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(rsmd.getColumnLabel(i), rs.getString(i));
                }
                return row;
            }
        }
        return null;
    }

    private String[] _fetchRow(ResultSet rs) throws SQLException {
        if (rs.next()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            if (columnCount > 0) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getString(i + 1);
                }
                return row;
            }
        }
        return null;
    }

    public ArrayList<HashMap<String, String>> fetchAll(String sql) {
        ArrayList<HashMap<String, String>> array = new ArrayList<HashMap<String, String>>();
        try {
            ResultSet rs = this._query(sql);
            HashMap<String, String> row = null;
            while (null != (row = this._fetchAssoc(rs))) {
                array.add(row);
            }
            rs.close();
        } catch (Exception e) {
            System.err.println(e.toString());
        } finally {

        }
        return array;
    }

    public ArrayList<String[]> fetchAll(String sql, Object numIndex) {
        ArrayList<String[]> array = new ArrayList<String[]>();
        try {
            ResultSet rs = this._query(sql);
            String[] row = null;
            while (null != (row = this._fetchRow(rs))) {
                array.add(row);
            }
            rs.close();
        } catch (Exception e) {
            System.err.println(e.toString());
        } finally {

        }
        return array;
    }

    public HashMap<String, String> fetchFirst(String sql) {
        HashMap<String, String> row = null;
        try {
            ResultSet rs = this._query(sql);
            row = this._fetchAssoc(rs);
            rs.close();
        } catch (Exception e) {
            System.err.println(e.toString());
        } finally {

        }
        return row;
    }

    public String[] fetchFirst(String sql, Object numIndex) {
        String[] row = null;
        try {
            ResultSet rs = this._query(sql);
            row = this._fetchRow(rs);
            rs.close();
        } catch (Exception e) {
            System.err.println(e.toString());
        } finally {

        }
        return row;
    }

    public String result(String sql) {
        String result = null;
        try {
            String[] row = this.fetchFirst(sql, true);
            if (null != row) {
                return row[0];
            }
        } catch (Exception e) {
            System.err.println(e.toString());
        } finally {

        }
        return result;
    }

}
