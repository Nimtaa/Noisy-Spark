import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class UpdateRecfromSpark {

    public static void main(String[] args) {
        JavaSparkContext js = new JavaSparkContext();
        js.setLogLevel("ERROR");
        JavaStreamingContext jssc = new JavaStreamingContext(js, new Duration(1000));
        jssc.checkpoint("/home/nima/Desktop/tempDir");
        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("127.0.0.1",9998);
        JavaDStream<String> records = dStream.flatMap(x-> Arrays.asList(x.split("\n")).iterator());

        JavaPairDStream<String,Integer> ctpair =  records
                .mapToPair(s->new Tuple2<>(s,Integer.parseInt(s.split(",")[2])));

        //TODO change to lambda expression
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                        Integer newSum = state.orElse(0);
                        return Optional.of(newSum);
                    }
                };
         JavaPairDStream<String,Integer> ctpairupdated = ctpair.updateStateByKey(updateFunction);
         ctpairupdated.print();

         jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
