package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class WordCountFromHBase {


    public static final String COLUMN_FAMILY_NAME = "cf";
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_NAME);
    public static final byte[] QUALIFIER = Bytes.toBytes("column");

    public static void wordCount(JavaSparkContext sparkContext, String inputTable,
                                 Configuration hbaseConf, String outputFolder) {

        Configuration conf = new Configuration(hbaseConf);
        conf.set(TableInputFormat.INPUT_TABLE, inputTable);
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseInput = sparkContext.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<String> inputLines = hbaseInput.map( row -> Bytes.toString(row._2.getValue(COLUMN_FAMILY, QUALIFIER)));

        JavaRDD<String> words = inputLines
             .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words
            .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile(outputFolder);
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
            .setAppName("Java RDD Word Counter (from HBase)");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Configuration hbaseConf = HBaseConfiguration.create();
        if(args.length > 2) {
            hbaseConf.set("hbase.zookeeper.quorum", args[2]);
        }

        try {
            wordCount(sparkContext, args[0], hbaseConf,  args[1]);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
