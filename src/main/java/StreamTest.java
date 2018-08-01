import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class StreamTest {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext();
        JavaStreamingContext jss = new JavaStreamingContext(jsc,new Duration(1000));

        //create DStream
        JavaReceiverInputDStream<String> numbers = jss.socketTextStream("localhost",9998);
        numbers.flatMap(n -> Arrays.asList(n.split("\n")).iterator());

        JavaPairDStream<String,Integer> numberCount = numbers.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        numberCount.reduceByKey((i1,i2) -> i1+i2);
        numberCount.print();

        jss.start();
        try {

            jss.awaitTermination();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


}
