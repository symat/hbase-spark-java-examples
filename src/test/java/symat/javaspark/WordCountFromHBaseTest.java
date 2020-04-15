package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static symat.javaspark.WordCountToHFile.COLUMN_FAMILY;
import static symat.javaspark.WordCountToHFile.QUALIFIER;

public class WordCountFromHBaseTest {


    JavaSparkContext sparkContext;
    Configuration hbaseConf;
    MiniHBaseCluster miniHBaseCluster;
    String outputFolderPath;

    private final HBaseTestingUtility testUtil = new HBaseTestingUtility();


    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf()
            .setMaster("local")
            .setAppName("Java RDD Word Counter");

        sparkContext = new JavaSparkContext(sparkConf);

        miniHBaseCluster = testUtil.startMiniCluster(1);
        hbaseConf = testUtil.getConfiguration();

        File testOutput = File.createTempFile("temp-out-folder", ".tmp");
        testOutput.delete();
        outputFolderPath = testOutput.getAbsolutePath();
        System.out.println("output folder: " + outputFolderPath);

    }

    @After
    public void tearDown() throws Exception {
        sparkContext.stop();

        miniHBaseCluster.shutdown();

        // recursive deletion of output folder
        if(new File(outputFolderPath).exists()) {
            Files.walk(Paths.get(outputFolderPath))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }


    @Test
    public void shouldCountWords() throws Exception {

        // GIVEN
        Table testTable = testUtil.createTable(TableName.valueOf("table-name"), COLUMN_FAMILY);

        writeDataToHBase(testTable, "this is the first line", "and this is the second line");

        // WHEN
        System.out.println("======= starting spark job ========");
        WordCountFromHBase.wordCount(sparkContext, "table-name", hbaseConf, outputFolderPath );
        System.out.println("======= spark job finished ========");


        // THEN
        Map<String, Integer> output = readOutputFile(outputFolderPath + "/part-00000");

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


    private void writeDataToHBase(Table testTable, String... lines) throws IOException {
        int rowKey = 0;
        for(String line : lines) {
            Put p = new Put(Bytes.toBytes(rowKey));
            byte[] value = Bytes.toBytes(line);
            p.addColumn(COLUMN_FAMILY, QUALIFIER, value);
            testTable.put(p);
            rowKey++;
        }
    }


    private Map<String, Integer> readOutputFile(String path) throws IOException {

        /* example file content:
        (this,2)
        (is,2)
        (second,1)
         */

        return Files.lines(Paths.get(path))
            .filter(s -> !s.isEmpty())
            .map(s -> s.substring(1, s.length()-1))
            .map(s -> s.split(","))
            .collect(Collectors.toMap(x -> x[0], x->Integer.valueOf(x[1])));
    }

}