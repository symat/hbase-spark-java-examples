package symat.javaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;


public class WordCounter {


    public static void wordCount(JavaSparkContext sparkContext, String inputFileName, String outputFolder) {

        JavaRDD<String> inputFile = sparkContext.textFile(inputFileName);

         JavaRDD<String> words = inputFile
             .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words
            .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile(outputFolder);
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
            .setAppName("Java RDD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        wordCount(sparkContext, args[0], args[1]);

    }
}
