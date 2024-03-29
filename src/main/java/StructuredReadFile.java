import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
                .csv("/home/nima/Desktop/temp");

        Dataset<Row> queryResult = csvRead.select("city").where("temperature > 35");

        Dataset<Integer> counting = queryResult.map(x->1,Encoders.INT());

//        StreamingQuery q = counting.writeStream()
//                .outputMode("append")
//                .format("csv")
//                .option("path","/home/nima/Desktop/temp")
//                .option("checkpointLocation","/home/nima/Desktop/temp")
//                .start();

        StreamingQuery query = queryResult.writeStream()
                .outputMode("append")
                .format("csv")
                .option("path","/home/nima/Desktop/temp")
                .option("checkpointLocation","/home/nima/Desktop/temp")
                .start();

        //System.out.println("Active : "+spark.streams().active());

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
//        try {
//            q.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }

    }



}
