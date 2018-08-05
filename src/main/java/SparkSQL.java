import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {

    public static void main(String[] args) {

        SparkSession spark =  SparkSession.builder().appName("SparkSQL").getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        String pathFile = "/home/nima/Desktop/DA-Grades.csv";

        Dataset<Row> grades = sqlContext.read().csv(pathFile);
        grades.printSchema();
        grades.cache();
        System.out.println(grades.count());
        sqlContext.registerDataFrameAsTable(grades,"gradeTable");
        sqlContext.sql("Select gradeTable._c1 from gradeTable where gradeTable._c8 >10").show();

    }
}
