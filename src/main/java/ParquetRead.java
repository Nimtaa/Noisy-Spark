import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;


public class ParquetRead {
    public static void main(String[] args) {

        String parquetPath = "/home/nima/Desktop/tempDir/parquet";
        StructType userSchema = new StructType()
                .add("timestamp","timestamp")
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
        

        StreamingQuery query = ps.writeStream()
                .outputMode("update")
                .format("console")
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

}
