package com.linkedin.venice.spark.datawriter.partition;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceSparkPartitionerTest {
  private SparkSession spark;
  private static final int TEST_PARTITION_COUNT = 10;

  private final Properties properties = new Properties();
  private Broadcast<Properties> broadcastProperties;
  private VeniceSparkPartitioner veniceSparkPartitioner;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder().appName("TestApp").master(SparkConstants.DEFAULT_SPARK_CLUSTER).getOrCreate();
    properties.setProperty(PARTITION_COUNT, String.valueOf(TEST_PARTITION_COUNT));

    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    broadcastProperties = sparkContext.broadcast(properties);
    veniceSparkPartitioner = new VeniceSparkPartitioner(broadcastProperties, TEST_PARTITION_COUNT);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    spark.stop();
  }

  @Test
  public void testGetPartition() {
    byte[] key = RandomUtils.nextBytes(4);
    byte[] value = RandomUtils.nextBytes(5);
    Row row = RowFactory.create(key, value);

    VenicePartitioner expectedPartitioner = new DefaultVenicePartitioner(new VeniceProperties(properties));
    Assert.assertEquals(
        veniceSparkPartitioner.getPartition(row),
        expectedPartitioner.getPartitionId(key, TEST_PARTITION_COUNT));

    int sprayedPartition = 3;
    byte[] sprayedKey = new byte[0];
    byte[] sprayedValue = new byte[Integer.BYTES];
    ByteUtils.writeInt(sprayedValue, sprayedPartition, 0);
    Row sprayedRow = RowFactory.create(sprayedKey, sprayedValue);

    Assert.assertEquals(veniceSparkPartitioner.getPartition(sprayedRow), sprayedPartition);
  }

  @Test
  public void testNumPartitions() {
    Assert.assertEquals(veniceSparkPartitioner.numPartitions(), TEST_PARTITION_COUNT);
  }

  @Test
  public void testGetPartitionWithIncorrectPartitionCount() {
    Assert.assertThrows(
        VeniceException.class,
        () -> new VeniceSparkPartitioner(broadcastProperties, TEST_PARTITION_COUNT + 10));
  }

  @Test
  public void testGetPartitionForNonUnsupportedInput() {
    Assert.assertThrows(VeniceException.class, () -> veniceSparkPartitioner.getPartition("row"));
  }
}
