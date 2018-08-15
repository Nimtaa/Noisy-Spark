import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
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
//                .withColumn("timestamp",
//                     unix_timestamp(split(col("value"),",").getItem(0),"yyyy-MM-dd HH:mm:ss.SSS")
//                .cast("Timestamp"))
                .withColumn("timestamp",
                        split(col("value"),",").getItem(0)
                                .cast("Timestamp"))
                .withColumn("city",split(col("value"),",").getItem(1))
                .withColumn("temperature",split(col("value"),",").getItem(2));
//        Dataset<Row> timestampsplitted = splitted.select
//                (unix_timestamp(splitted.col("timestamp")).cast(TimestampType).as("timestamp"),"city","temperature");


        Dataset<Row> counting = splitted.groupBy("timestamp").count();
        Dataset<Row> withoutValue = splitted.drop(col("value"));
//
//        Dataset<Row> windowedCounts = withoutValue
//                .withWatermark("timestamp", "1 minutes")
//                .groupBy(
//                        functions.window(withoutValue.col("timestamp"), "1 minutes", "30 seconds"),
//                        withoutValue.col("timestamp"))
//                .count();
        Dataset<Row> result = withoutValue.withWatermark("timestamp","1 minutes")
                .groupBy("timestamp").count();

        //Dataset<Row> queryResult = withoutValue.select("*").where("temperature > 35");

        StreamingQuery query = result.writeStream()
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

//    static Column to_timestamp(Column col){
//
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
//        Date parsedDate = null;
//        try {
//            parsedDate = dateFormat.parse(col.toString());
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
//
//    }
}
