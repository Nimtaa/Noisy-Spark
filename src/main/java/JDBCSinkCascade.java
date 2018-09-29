
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
public class JDBCSinkCascade extends ForeachWriter<Row> {

    PreparedStatement pstmt  = null ;
    PreparedStatement pstmt2  = null ;
    PreparedStatement pstmt3 = null;
    Connection conn = null ;
    String upsertCitytempTable = "insert into citytemp (date,city,temperature) values (?,?,?)"
            + "ON DUPLICATE KEY UPDATE temperature = IF(date < VALUES (date), VALUES (temperature),temperature),"
            + "date = IF(date < VALUES (date), VALUES (date),date);";

    String updateChildTable = "insert into childagain (date,city,temperature) values (?,?,?)"
            +"ON DUPLICATE KEY UPDATE temperature = IF(date < VALUES (date), VALUES (temperature),temperature)," +
            "date = IF(date < VALUES (date), VALUES (date),date);";
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
            conn.setAutoCommit(false);//transaction block start

            pstmt = conn.prepareStatement(upsertCitytempTable);
            pstmt.setString(1,row.get(0).toString().split(",")[0]);
            pstmt.setString(2, row.get(0).toString().split(",")[1]);
            pstmt.setInt(3, Integer.parseInt(row.get(0).toString().split(",")[2]));
            pstmt.executeUpdate();
            conn.commit();//transaction block end

            conn.setAutoCommit(false);
            pstmt2 = conn.prepareStatement(updateChildTable);
            pstmt3 = conn.prepareStatement("select * from citytemp where temperature>40");
            ResultSet rs = pstmt3.executeQuery();
            while (rs.next()){
                pstmt2.setString(1,rs.getString("date"));
                pstmt2.setString(2, rs.getString("city"));
                pstmt2.setInt(3, rs.getInt("temperature"));
                pstmt2.executeUpdate();
            }

            conn.commit();

        }
            catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable throwable) {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
