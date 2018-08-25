import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class ParquetTest {

    public static void main(String[] args) {
        SparkSession spark =  SparkSession
                .builder()
                .appName("parquetTest")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> data = spark.readStream()
                .format("socket")
                .option("host","127.0.0.1")
                .option("port",9998)
                .load();
        data.as(Encoders.STRING())
        .flatMap((FlatMapFunction<String,String>) x -> Arrays.asList(x.split("\n")).iterator(), Encoders.STRING());
        Dataset<Row> splitted = data
                .withColumn("timestamp",
                        split(col("value"),",").getItem(0).cast("Timestamp"))
                .withColumn("city",split(col("value"),",").getItem(1))
                .withColumn("temperature",split(col("value"),",").getItem(2));
        Dataset<Row> withoutValue = splitted.drop(col("value"));


        StreamingQuery query =  withoutValue.writeStream()
                .format("parquet")
                .option("checkpointLocation","/home/nima/Desktop/tempDir/parquetPartition")
                .option("path","/home/nima/Desktop/tempDir/parquetPartition")
                .partitionBy("city")
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
