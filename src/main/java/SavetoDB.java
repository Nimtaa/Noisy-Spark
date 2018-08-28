import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import java.sql.*;
import java.util.Arrays;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class SavetoDB extends ForeachWriter<Row> {
    PreparedStatement pstmt = null ;
    Connection conn = null ;
    SparkSession spark = null;

    public SavetoDB(){
        spark = SparkSession.builder().appName("savetodb").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
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
            pstmt = conn.prepareStatement("select into citytemp values (?,?,?)");
            //pstmt.setTimestamp(1, Timestamp.valueOf( col("timestamp").toString()));
            pstmt.setString(1,row.get(0).toString());
            pstmt.setString(2, row.get(1).toString());
            pstmt.setInt(3, 24);
            pstmt.executeUpdate();
            System.out.println("query executed");
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

    public static void main(String[] args)  {

        ForeachWriter<Row> writer = new SavetoDB();

        Dataset<Row> europeTemp = ((SavetoDB) writer).spark.readStream().format("socket")
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

        splitted.writeStream().foreach(writer).start();


    }

}
