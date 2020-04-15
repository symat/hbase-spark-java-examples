# Example projects to use Spark with HBase

Each of the classes are specifying a simple Spark job that interacts with HBase in some ways.
Unit tests are created for each spark job, using local HBase minicluster.

- `WordCounter`: simple hello world Spark application, reads a text file from local disk (or HDFS) and writes the (word, count) pairs to an other text file
- `WordCountFromHBase`: reading the input strings from HBase, then writes the (word, count) pairs to a text file
- `WordCountToHBase`: reading the input from text file, then writing the results to HBase
- `WordCountToHFile`: reading the input from text file, then writing the results in HFile format to a folder
- `WordCountBulkLoadToHBase`: reading the input from text file, then writing the results in HFile format to a folder, using it later to bulkload the data into HBase

Currently I only added Spark Core (RDD) jobs, I plan to add examples with Spark SQL (DataFrame/DataSet) API later.

## Submit the jobs to a real cluster

I'm using a Cloudera CDP 7.1 cluster, on other distributions the commands might be a bit different.

Now in production, you have to submit the spark job with a user having
its home folder and also having access to Yarn. In this example I will simply use the `hdfs` user.

```
# build the fat jar
# (you might not need a fat jar, depending on what jars / libs
#  are available for spark on the cluster)
mvn clean package -DskipTests


# scp the code to a cluster, where the hdfs user can reach it
scp target/hbase-spark-java-examples-1.0-SNAPSHOT-jar-with-dependencies.jar  root@my_cluster:/tmp/spark-hbase.jar


# ssh to your cluster
ssh root@my_cluster
chmod 777 /tmp/spark-hbase.jar


# create an input file on hdfs
echo "my wordcount test data" > test.txt
echo "and some other test data" >> test.txt
hdfs dfs -copyFromLocal ./test.txt /tmp/test.txt



# submit the simple word counter spark job to Yarn in client mode 
# (check the arguments for each job in the main function)
sudo -u hdfs spark-submit --class symat.javaspark.WordCounter --master yarn --deploy-mode client /tmp/spark-hbase.jar  hdfs:///tmp/test.txt hdfs:///tmp/test-output

# check the output
hdfs dfs -ls /tmp/test-output/
hdfs dfs -cat /tmp/test-output/part-00000



# prepare the test table in HBase
hbase shell
> create 'testtable', 'cf'


# submit the bulkload word counter spark job in cluster mode, using Yarn 
# (check the arguments for each job in the main function)
# you have to use a user that has access to Yarn in HDFS
sudo -u hdfs spark-submit --class symat.javaspark.WordCountBulkLoadToHBase --master yarn --deploy-mode cluster /root/spark-hbase.jar  hdfs:///tmp/test.txt testtable


# verify the output in HBase
hbase shell
> scan 'testtable'

```


## Further interesting readings

- https://richardstartin.github.io/posts/co-locating-spark-partitions-with-hbase-regions
- https://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/
- https://blog.cloudera.com/spark-hbase-dataframe-based-hbase-connector/
- https://docs.cloudera.com/documentation/enterprise/latest/topics/cdh_ig_running_spark_on_yarn.html

## License

The good old Apache 2.0 license.