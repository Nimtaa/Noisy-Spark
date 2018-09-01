import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.sql.*;

public class JDBCSink extends ForeachWriter<Row> {
    PreparedStatement pstmt = null ;
    Connection conn = null ;
    public static Connection getConnection()  {
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost/testdb";
        String username = "phpmyadmin";
        String password = "Nima9112543378";
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    @Override
    public boolean open(long l, long l1) {
        conn = getConnection();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }
    @Override
    public void process(Row row) {
        try {
            pstmt = conn.prepareStatement("insert into citytemp (date,city,temperature) values (?,?,?)" +
                    " ON DUPLICATE KEY UPDATE temperature=VALUES(temperature), date =VALUES(date) ;");
            //pstmt.setTimestamp(1, Timestamp.valueOf( col("timestamp").toString()));
            //because we have value column this gest other data from value column
            pstmt.setString(1,row.get(0).toString().split(",")[0]);
            pstmt.setString(2, row.get(0).toString().split(",")[1]);
            pstmt.setInt(3, Integer.parseInt(row.get(0).toString().split(",")[2]));
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void close(Throwable throwable) {
        try {
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
