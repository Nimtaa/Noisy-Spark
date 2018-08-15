import org.apache.spark.sql.*;


public class RateCalc {


    public static void main(String[] args) {
        String dirPath = "/home/nima/Desktop/temp/*.csv";
        String outPath = "/home/nima/Desktop/temp/out.csv";
        SparkSession sparkSession = SparkSession.builder().appName("RateCalculation").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);


        Dataset<Row> files = sparkSession.read().csv(dirPath);
        files.printSchema();

        Dataset<Row> outsecond  = files.withColumn("milisecond",(files.col("_c0")));
        outsecond.show();

//        outsecond.coalesce(1).write().csv(outPath);


        try {
            files.createGlobalTempView("times");

            sqlContext.sql("select avg(_c1) from global_temp.times").show();

        } catch (AnalysisException e) {
            e.printStackTrace();
        }

    }

}
