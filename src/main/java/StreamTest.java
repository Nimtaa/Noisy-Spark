import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamTest {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext();
        JavaStreamingContext jss = new JavaStreamingContext(jsc,new Duration(100));

        
    }

}
