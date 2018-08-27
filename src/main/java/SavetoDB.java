import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.sql.*;
import java.text.DateFormat;
import java.util.Arrays;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.soundex;
import static org.apache.spark.sql.functions.split;


public class SavetoDB {

    public static void main(String[] args) throws SQLException {

        SparkSession spark = SparkSession.builder().appName("savetodb").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        PreparedStatement pstmt = null;
        Connection conn = null;
        Dataset<Row> europeTemp = spark.readStream().format("socket")
                .option("host","127.0.0.1")
                .option("port",9998)
                .load();

        europeTemp
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>) x -> Arrays.asList(x.split("\n")).iterator(), Encoders.STRING());


        Dataset<Row> splitted = europeTemp
                .withColumn("timestamp",
                        split(col("value"),",").getItem(0)
                                .cast("Timestamp"))
                .withColumn("city",split(col("value"),",").getItem(1))
                .withColumn("temperature",split(col("value"),",").getItem(2));

        //Direct Way using write from spark
//        splitted.write().format("jdbc")
//                .option("")

        //Indirect way using database connection and preparestatement
        conn = getConnection();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            pstmt = conn.prepareStatement("insert into citytemp(date, city ,temperature) values (?, ?, ?);");
            pstmt.setTimestamp(1, Timestamp.valueOf( splitted.col("timestamp").toString()));
            pstmt.setString(2, splitted.col("city").toString());
            pstmt.setInt(3, 24);
//            pstmt = conn.prepareStatement("insert  into citytemp values (?,?,?);");
//            pstmt.setString(1,null);
//            pstmt.setString(2,"sari");
//            pstmt.setInt(3,12);
            pstmt.executeUpdate();
            conn.commit();
            System.out.println("query executed");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
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
}
