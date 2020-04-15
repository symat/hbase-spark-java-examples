package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;


public class WordCountBulkLoadToHBase {


    public static final String COLUMN_FAMILY_NAME = "cf";
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_NAME);
    public static final byte[] QUALIFIER = Bytes.toBytes("column");

    public static void wordCount(JavaSparkContext sparkContext, String inputFileName,
                                 Configuration hbaseConf, String outputTable) throws IOException {

        JavaRDD<String> inputFile = sparkContext.textFile(inputFileName);

        JavaRDD<String> words = inputFile
             .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words
            .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);


        // we need to sort the records by keys in the HFile
        // we would also need to partition by the region boundaries (currently we simply use a single partition)
        // we could also give a custom comparator to repartitionAndSortWithinPartitions to introduce salting
        JavaPairRDD<String, Integer> sortedCounts = counts.repartitionAndSortWithinPartitions(new HashPartitioner(1));

        // we need to convert our data to an RDD with type of <rowKey bytes, KeyValue>
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileContent = sortedCounts.mapToPair(wordAndCount -> {
            byte[] rowKey = Bytes.toBytes(wordAndCount._1);
            byte[] value = Bytes.toBytes(wordAndCount._2);
            KeyValue keyValue = new KeyValue(rowKey, COLUMN_FAMILY, QUALIFIER, value);
            return new Tuple2<>(new ImmutableBytesWritable(rowKey), keyValue);
        });


        Configuration config = updateConfigBasedOnHBaseTable(hbaseConf, outputTable);

        hFileContent.saveAsNewAPIHadoopFile("hdfs:///tempfolder_in_hdfs", ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, config);

        bulkloadToHbase(outputTable, config, "hdfs:///tempfolder_in_hdfs");

    }

    // set the compression and block encoding based on the current settings for the table
    private static Configuration updateConfigBasedOnHBaseTable(Configuration hbaseConf, String outputTable) throws IOException {
        Connection hBaseConection = ConnectionFactory.createConnection(hbaseConf);
        Table table = hBaseConection.getTable(TableName.valueOf(outputTable));
        RegionLocator regionLocator = hBaseConection.getRegionLocator(TableName.valueOf(outputTable));
        try {
            Job jobWeOnlyUseForConfig = new Job(hbaseConf, "job name");
            HFileOutputFormat2.configureIncrementalLoad(jobWeOnlyUseForConfig, table, regionLocator);
            return jobWeOnlyUseForConfig.getConfiguration();
        } finally {
            hBaseConection.close();
            table.close();
            regionLocator.close();
        }
    }

    private static void bulkloadToHbase(String outputTable, Configuration config, String hFileFolder) throws IOException {
        Connection hBaseConection = ConnectionFactory.createConnection(config);
        Table table = hBaseConection.getTable(TableName.valueOf(outputTable));
        RegionLocator regionLocator = hBaseConection.getRegionLocator(TableName.valueOf(outputTable));
        Admin admin = hBaseConection.getAdmin();
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
        try {
            loader.doBulkLoad(new Path(hFileFolder), admin, table, regionLocator);
        } finally {
            hBaseConection.close();
            table.close();
            admin.close();
            regionLocator.close();
        }
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
            .setAppName("Java RDD Word Counter (bulkload)");

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
