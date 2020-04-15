package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;


public class WordCountToHFile {


    public static final String COLUMN_FAMILY_NAME = "cf";
    public static final byte[] COLUMN_FAMILY = COLUMN_FAMILY_NAME.getBytes();
    public static final byte[] QUALIFIER = "column".getBytes();

    public static void wordCount(JavaSparkContext sparkContext, String inputFileName, String outputFolder,
                                 Configuration hbaseConf, String outputTable) throws IOException {

        JavaRDD<String> inputFile = sparkContext.textFile(inputFileName);

         JavaRDD<String> words = inputFile
             .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words
            .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        // we need to sort the records by keys in the HFile
        // we can also partition by custom partitioner using repartitionAndSortWithinPartitions
        JavaPairRDD<String, Integer> sortedCounts = counts.sortByKey();

        // we need to convert our data to an RDD with type of <rowKey bytes, KeyValue>
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileContent = sortedCounts.mapToPair( wordAndCount -> {
            byte[] rowKey = Bytes.toBytes(wordAndCount._1);
            byte[] value = Bytes.toBytes(wordAndCount._2);
            KeyValue keyValue = new KeyValue(rowKey, COLUMN_FAMILY, QUALIFIER, value);
            return new Tuple2<>(new ImmutableBytesWritable(rowKey), keyValue);
        });


        Configuration config = new Configuration(hbaseConf);
        config.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
        config.set("mapreduce.job.output.key.class", "org.apache.hadoop.hbase.io.ImmutableBytesWritable");
        config.set("mapreduce.job.output.value.class", "org.apache.hadoop.hbase.KeyValue");
        config.set("hbase.mapreduce.hfileoutputformat.table.name", outputTable);


        hFileContent.saveAsNewAPIHadoopFile(outputFolder, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, config);
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
            .setAppName("Java RDD Word Counter (to HFile)");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Configuration hbaseConf = HBaseConfiguration.create();
        if(args.length > 3) {
            hbaseConf.set("hbase.zookeeper.quorum", args[3]);
        }

        try {
            wordCount(sparkContext, args[0], args[1], hbaseConf, args[2]);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
