package symat.javaspark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.Assert.assertTrue;
import static symat.javaspark.WordCountToHFile.COLUMN_FAMILY;
import static symat.javaspark.WordCountToHFile.COLUMN_FAMILY_NAME;

public class WordCountToHFileTest {


    JavaSparkContext sparkContext;
    String outputFolderPath;
    Configuration hbaseConf;
    MiniHBaseCluster miniHBaseCluster;

    private final HBaseTestingUtility testUtil = new HBaseTestingUtility();


    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf()
            .setMaster("local")
            .setAppName("Java RDD Word Counter");

        sparkContext = new JavaSparkContext(sparkConf);

        File testOutput = File.createTempFile("temp-out-folder", ".tmp");
        testOutput.delete();
        outputFolderPath = testOutput.getAbsolutePath();
        System.out.println("output folder: " + outputFolderPath);


        miniHBaseCluster = testUtil.startMiniCluster(1);
        hbaseConf = testUtil.getConfiguration();
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
        testUtil.createTable(TableName.valueOf("table-name"), COLUMN_FAMILY);

        String testInput = writeTestFile("this is the first line",
                                         "and this is the second line");

        // WHEN
        WordCountToHFile.wordCount(sparkContext, testInput, "file://"+outputFolderPath, hbaseConf, "table-name");


        // THEN: there is a success indicator file present
        assertTrue(Files.exists(Paths.get(outputFolderPath, "_SUCCESS")));

        // THEN: there is a folder for the column family
        assertTrue(Files.exists(Paths.get(outputFolderPath, COLUMN_FAMILY_NAME)));

        // THEN: there is exactly a single HFile present in the column family folder
        assertTrue(Files.list(Paths.get(outputFolderPath, COLUMN_FAMILY_NAME)).count() == 1);
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


}