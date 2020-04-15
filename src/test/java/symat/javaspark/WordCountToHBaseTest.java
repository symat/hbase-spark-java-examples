package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static symat.javaspark.WordCountToHFile.COLUMN_FAMILY;
import static symat.javaspark.WordCountToHFile.QUALIFIER;

public class WordCountToHBaseTest {


    JavaSparkContext sparkContext;
    Configuration hbaseConf;
    MiniHBaseCluster miniHBaseCluster;

    private final HBaseTestingUtility testUtil = new HBaseTestingUtility();


    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf()
            .setMaster("local")
            .setAppName("Java RDD Word Counter");

        sparkContext = new JavaSparkContext(sparkConf);

        miniHBaseCluster = testUtil.startMiniCluster(1);
        hbaseConf = testUtil.getConfiguration();
    }

    @After
    public void tearDown() throws Exception {
        sparkContext.stop();

        miniHBaseCluster.shutdown();
    }


    @Test
    public void shouldCountWords() throws Exception {

        // GIVEN
        Table testTable = testUtil.createTable(TableName.valueOf("table-name"), COLUMN_FAMILY);

        String testInput = writeTestFile("this is the first line",
                                         "and this is the second line");

        // WHEN
        System.out.println("======= starting spark job ========");
        WordCountToHBase.wordCount(sparkContext, testInput, hbaseConf, "table-name");
        System.out.println("======= spark job finished ========");


        // THEN
        Map<String, Integer> output = readOutputTable(testTable);

        Map<String, Integer> expectedOutput = new HashMap<>();
        expectedOutput.put("this", 2);
        expectedOutput.put("is", 2);
        expectedOutput.put("the", 2);
        expectedOutput.put("line", 2);
        expectedOutput.put("and", 1);
        expectedOutput.put("first", 1);
        expectedOutput.put("second", 1);


        assertEquals(expectedOutput, output);

     }




    private String writeTestFile(String... lines) throws IOException {
        File temp = File.createTempFile("temp-in-file", ".tmp");
        temp.deleteOnExit();

        BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
        for (String line : lines) {
            bw.write(line);
            bw.newLine();
        }
        bw.close();
        return temp.getAbsolutePath();
    }



    private Map<String, Integer> readOutputTable(Table testTable) throws IOException {

        Scan scan = new Scan();
        scan.addColumn(COLUMN_FAMILY, QUALIFIER);
        ResultScanner rs = testTable.getScanner(scan);

        Result next = rs.next();
        Map<String, Integer> results = new HashMap<>();
        while (next != null) {
            String word = Bytes.toString(next.getRow());
            Integer count = Bytes.toInt(next.getValue(COLUMN_FAMILY, QUALIFIER));
            results.put(word, count);
            next = rs.next();
        }
        rs.close();
        return results;
    }

    private void writeDataToHBase(Table testTable, Map<String, Integer> data) throws IOException {
        for(String word : data.keySet()) {
            Put p = new Put(Bytes.toBytes(word));
            byte[] value = Bytes.toBytes(data.get(word));
            p.addColumn(COLUMN_FAMILY, QUALIFIER, value);
            testTable.put(p);
        }
    }

}