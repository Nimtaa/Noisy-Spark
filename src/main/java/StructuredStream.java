import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;


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






        Dataset<Row> result = europeTemp.withColumn("city",
           );

        JavaPairRDD<String,Integer> data = europeTemp.as(Encoders.STRING()).toJavaRDD().mapToPair
                (new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String,Integer> call(String s) throws Exception {
                        return new Tuple2<String,Integer>(s.split(",")[0],Integer.parseInt(s.split(",")[1]));
                    }
                });
        //JavaRDD rowrdd = data.map(tuple -> RowFactory.create(tuple._1(),tuple._2()));


        JavaRDD<String> cityRdd = data.map(x->x._1());
        JavaRDD<Integer> tempRdd = data.map(x->x._2());

        //Dataset<Row> city = spark.createDataFrame(cityRdd,null);


        Dataset<Row> result = spark.createDataset(
        JavaPairRDD.toRDD(data), Encoders.tuple(Encoders.STRING(),Encoders.INT())).toDF("city","temp");


        StreamingQuery query = result.writeStream()
                .outputMode("update")
                .format("console")
                .start();
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
