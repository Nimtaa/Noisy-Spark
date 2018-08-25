import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;


public class ParquetRead {
    public static void main(String[] args) {

        String parquetPath = "/home/nima/Desktop/tempDir/parquet";
        StructType userSchema = new StructType()
                .add("timestamp","Timestamp")
                .add("city","string")
                .add("temperature","integer");
        SparkSession spark = SparkSession.builder()
                .appName("parquetread")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ps  = spark
                .readStream()
                .schema(userSchema)
                .parquet(parquetPath);

        Dataset<Row> windowedCount = ps.withWatermark("timestamp", "5 seconds")
                .groupBy(
                        functions.window(col("timestamp"), "1 minutes", "30 seconds"),
                        col("timestamp"))
                .count();

        StreamingQuery query = windowedCount.writeStream().format("console").outputMode("update")
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

}
