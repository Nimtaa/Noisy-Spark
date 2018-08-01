
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;


public class Main {
    public static void main(String[] args) {

        String logFile = "/home/nima/Desktop/tempDir/tempFile"; // Should be some file on your system
        JavaSparkContext jsp = new JavaSparkContext();

        JavaRDD<String> lines = jsp.textFile(logFile);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordcount = words.mapToPair
                (new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        JavaPairRDD<String,Integer> count = wordcount.reduceByKey((x,y) -> x+y);
        count.saveAsTextFile("/home/nima/Desktop/Out");
        jsp.stop();

    }
}
