import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
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
        JavaReceiverInputDStream<String> lines = jss.socketTextStream("127.0.0.1",9998);
        JavaDStream<String> words = lines.flatMap(n -> Arrays.asList(n.split(" ")).iterator());
        JavaPairDStream<String,Integer> wordCount = words.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> wc = wordCount.reduceByKey((i1,i2) -> i1+i2);

        System.out.println("THIS is output:");
        System.out.println(words.count());
        System.out.println("word count print");
        wc.print();
        jss.start();
        try {
            jss.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
