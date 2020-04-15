package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class WordCountToHBase {


    public static final String COLUMN_FAMILY_NAME = "cf";
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_NAME);
    public static final byte[] QUALIFIER = Bytes.toBytes("column");

    public static void wordCount(JavaSparkContext sparkContext, String inputFileName,
                                 Configuration hbaseConf, String outputTable) {

        JavaRDD<String> inputFile = sparkContext.textFile(inputFileName);

        JavaRDD<String> words = inputFile
             .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words
            .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        // we need to convert our data to an RDD with type of <rowKey bytes, Put>
        JavaPairRDD<ImmutableBytesWritable, Put> hFileContent = counts.mapToPair( wordAndCount -> {
            byte[] rowKey = Bytes.toBytes(wordAndCount._1);
            byte[] value = Bytes.toBytes(wordAndCount._2);
            Put put = new Put(rowKey);
            put.addColumn(COLUMN_FAMILY, QUALIFIER, value);
            return new Tuple2<>(new ImmutableBytesWritable(rowKey), put);
        });


        JobConf config = new JobConf(hbaseConf, WordCountToHBase.class);
        config.setOutputFormat(TableOutputFormat.class);
        config.set(TableOutputFormat.OUTPUT_TABLE, outputTable);

        hFileContent.saveAsHadoopDataset(config);
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
            .setAppName("Java RDD Word Counter (to HBase)");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Configuration hbaseConf = HBaseConfiguration.create();
        if(args.length > 2) {
            hbaseConf.set("hbase.zookeeper.quorum", args[2]);
        }

        try {
            wordCount(sparkContext, args[0], hbaseConf, args[1]);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
