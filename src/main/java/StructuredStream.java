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



        Dataset<Row> splitted = europeTemp.withColumn("city",split(col("value"),",").getItem(0))
               .withColumn("temperature",split(col("value"),",").getItem(1));


        Dataset<Row> counting = splitted.groupBy("city").count();
        Dataset<Row> withoutValue  = splitted.drop(col("value"));


        Dataset<Row> queryResult = withoutValue.select("*").where("temperature > 35");

//        StreamingQuery query = queryResult.writeStream()
//              .outputMode("append")
//              .format("console")
//              .start();

        StreamingQuery q = counting.writeStream()
              .outputMode("complete")
              .format("console")
              .start();
        try {
            q.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

//
//        try {
//            query.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }

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
