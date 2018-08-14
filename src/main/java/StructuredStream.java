import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;


public class StructuredStream {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType userSchema = new StructType().add("city","string").add("temperature","integer");
        Dataset<Row> lines = spark.readStream().format("socket")
                .option("host","127.0.0.1")
                .option("port",9998)
                .load();
        Dataset<Row> europeTemp = spark.readStream().format("socket")
                .option("host","127.0.0.1")
                .option("port",9998)
                .load();

        europeTemp
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>) x -> Arrays.asList(x.split("\n")).iterator(), Encoders.STRING());

        Dataset<Row> splitted = europeTemp
                .withColumn("timestamp",split(col("value"),",").getItem(0))
                .withColumn("city",split(col("value"),",").getItem(1))
                .withColumn("temperature",split(col("value"),",").getItem(2));


        Dataset<Row> tostampsplitted = splitted.select
                (unix_timestamp(splitted.col("timestamp")).cast(TimestampType).as("timestamp"),"city","temperature");


        Dataset<Row> counting = splitted.groupBy("timestamp").count();
        Dataset<Row> withoutValue = splitted.drop(col("value"));

        Dataset<Row> windowedCounts = splitted
                .withWatermark("timestamp", "1 minutes")
                .groupBy(
                        functions.window(splitted.col("timestamp"), "1 minutes", "1 minutes"),
                        splitted.col("timestamp"))
                .count();

        Dataset<Row> queryResult = withoutValue.select("*").where("temperature > 35");

        StreamingQuery query = withoutValue.writeStream()
                .outputMode("append")
                .format("csv")
                .option("path","/home/nima/Desktop/temp")
                .option("checkpointLocation","/home/nima/Desktop/temp")
                .start();

//        StreamingQuery q = counting.writeStream()
//              .outputMode("complete")
//              .format("console")
//              .start();
//        try {
//            q.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }


        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

//        Dataset<String> words = lines.as(Encoders.STRING())
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
//
//        Dataset<Row> wordCounts = words.groupBy("value").count();
//        StreamingQuery query = wordCounts.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .start();
//        try {
//            query.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }
    }
}
