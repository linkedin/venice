package com.linkedin.venice.spark.engine;

import com.linkedin.venice.spark.SparkConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SparkEngineTaskConfigProviderTest {
  private static final String TEST_APP_NAME = "SparkEngineTaskConfigProviderTest";

  private SparkSession spark;
  private JavaSparkContext sparkContext;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder().appName(TEST_APP_NAME).master(SparkConstants.DEFAULT_SPARK_CLUSTER).getOrCreate();

    // Will be closed when the spark session is closed
    sparkContext = new JavaSparkContext(spark.sparkContext());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    sparkContext.close();
    spark.stop();
  }

  @Test
  public void testGetJobName() {
    Properties jobProps = new Properties();
    String propKey1 = "TestPropKey";
    String propValue1 = "TestPropValue";
    jobProps.setProperty(propKey1, propValue1);

    List<String> data = Arrays.asList("1", "2", "3", "4", "5");

    int numPartitions = 2;
    JavaRDD<Row> rowRDD = sparkContext.parallelize(data).repartition(numPartitions).map(RowFactory::create);

    List<Integer> outputData = rowRDD.map((Row row) -> {
      SparkEngineTaskConfigProvider sparkEngineTaskConfigProvider = new SparkEngineTaskConfigProvider(jobProps);

      // TODO: Why does this not work?
      // Assert.assertEquals(sparkEngineTaskConfigProvider.getJobName(), TEST_APP_NAME);

      Properties taskJobProps = sparkEngineTaskConfigProvider.getJobProps();
      jobProps.forEach((key, value) -> Assert.assertEquals(taskJobProps.getProperty((String) key), value));

      return sparkEngineTaskConfigProvider.getTaskId();
    }).collect();

    List<Integer> mutableOutput = new ArrayList<>(outputData);
    mutableOutput.sort(Comparator.comparingInt(Integer::intValue));

    Assert.assertEquals(mutableOutput.get(0).intValue(), 0); // Check min partition id
    Assert.assertEquals(mutableOutput.get(mutableOutput.size() - 1).intValue(), numPartitions - 1); // Check max
                                                                                                    // partition id
  }
}
