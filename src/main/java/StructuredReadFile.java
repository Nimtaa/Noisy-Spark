import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class StructuredReadFile {

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("StructuredStreamReadFile")
                .getOrCreate();

        StructType schema = new StructType().add("city","string").add("temperature","integer");
        Dataset<Row> csvRead = spark.readStream()
                .option("sep",",")
                .schema(schema)
                .csv("/home/nima/Desktop/tempDir");

        Dataset<Row> queryResult = csvRead.select("*").where("temperature > 35");
        StreamingQuery query = queryResult.writeStream()
                .outputMode("append")
                .format("csv")
                .option("path","/home/nima/Desktop/tempDir")
                .option("checkpointLocation","/home/nima/Desktop/tempDir")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }



}
