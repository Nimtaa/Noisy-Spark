import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class SavetoDB  {


    public static void main(String[] args)  {
        SparkSession spark = SparkSession.builder().appName("savetodb").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        System.out.println(spark.conf().getAll());
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


        StreamingQuery query = splitted.writeStream().foreach(new JDBCSink())
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
