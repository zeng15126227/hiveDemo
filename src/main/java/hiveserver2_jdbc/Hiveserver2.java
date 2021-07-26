package hiveserver2_jdbc;

import java.sql.*;

public class Hiveserver2 {
    private static String driverName ="org.apache.hive.jdbc.HiveDriver";
    private static String Url="jdbc:hive2://172.16.21.114:10000/default";
    private static Connection conn;
    private static PreparedStatement pstmt;
    private static ResultSet res;

    public static void main(String[] args) {
        conn = getConnnection();
        String sql = "show tables";
        pstmt = prepare(conn,sql);
        try {
            res = pstmt.executeQuery();
            while (res.next()){
                System.out.println(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                res.close();
                pstmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Connection getConnnection()
    {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(Url,"root","123");
        } catch(ClassNotFoundException e)  {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static PreparedStatement prepare(Connection conn, String sql) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }
}
