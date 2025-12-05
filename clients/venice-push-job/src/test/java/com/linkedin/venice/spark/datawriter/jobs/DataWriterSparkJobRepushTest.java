package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.spark.SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.writer.TestSparkPartitionWriter;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.Test;


/**
 * End-to-end integration tests for Spark repush functionality.
 * Overrides getKafkaInputDataFrame() and createPartitionWriterFactory() in AbstractDataWriterSparkJob.
 * Uses TestSparkPartitionWriter to capture output instead of writing to Kafka.
 *
 */
public class DataWriterSparkJobRepushTest {
  @Test
  public void testRunComputeJobEndToEnd() {
    String testName = "testRunComputeJobEndToEnd";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    TestDataWriterSparkJob job = new TestDataWriterSparkJob(testName);

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.kafkaInputBrokerUrl = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.kafkaUrl = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;

    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);

    job.runComputeJob();

    List<TestSparkPartitionWriter.TestRecord> capturedRecords = TestSparkPartitionWriter.getCapturedRecords(testName);
    assertEquals(capturedRecords.size(), 2, "Should have captured 2 records");

    TestSparkPartitionWriter.TestRecord record1 = capturedRecords.get(0);
    assertTrue(Arrays.equals(record1.key, "test-key-1".getBytes()), "First key should match");
    assertTrue(Arrays.equals(record1.value, "test-value-1".getBytes()), "First value should match");
    assertTrue(Arrays.equals(record1.rmd, "rmd".getBytes()), "First RMD should match");

    TestSparkPartitionWriter.TestRecord record2 = capturedRecords.get(1);
    assertTrue(Arrays.equals(record2.key, "test-key-2".getBytes()), "Second key should match");
    assertTrue(Arrays.equals(record2.value, "test-value-2".getBytes()), "Second value should match");
    assertTrue(Arrays.equals(record2.rmd, "rmd".getBytes()), "Second RMD should match");
  }

  /**
   * Test with DELETE messages through runComputeJob().
   */
  @Test
  public void testRunComputeJobWithDeleteMessages() {
    String testName = "testRunComputeJobWithDeleteMessages";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    TestDataWriterSparkJobWithDeletes job = new TestDataWriterSparkJobWithDeletes(testName);

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.kafkaInputBrokerUrl = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.kafkaUrl = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;

    // Create mock Version for source kafka input
    Version sourceVersion = new VersionImpl("test_store", 1, "test-push-id");
    setting.sourceKafkaInputVersionInfo = sourceVersion;

    job.configure(new VeniceProperties(props), setting);

    job.runComputeJob();

    // Verify DELETE message
    List<TestSparkPartitionWriter.TestRecord> capturedRecords = TestSparkPartitionWriter.getCapturedRecords(testName);
    assertEquals(capturedRecords.size(), 1, "Should have captured 1 DELETE record");

    TestSparkPartitionWriter.TestRecord record = capturedRecords.get(0);
    assertTrue(Arrays.equals(record.key, "delete-key".getBytes()), "DELETE key should match");
    assertTrue(Arrays.equals(record.value, new byte[0]), "DELETE value should be empty");
    assertTrue(Arrays.equals(record.rmd, "rmd".getBytes()), "DELETE RMD should match");
  }

  private Properties createDefaultTestProperties() {
    Properties props = new Properties();
    props.setProperty(KAFKA_INPUT_TOPIC, "test_store_v1");
    props.setProperty(KAFKA_INPUT_BROKER_URL, "localhost:9092");
    props.setProperty(TOPIC_PROP, "test_store_v1");
    props.setProperty(PARTITION_COUNT, "1");
    // Configure Venice writer properties to avoid validation errors
    props.setProperty("venice.writer.max.record.size.bytes", String.valueOf(Integer.MAX_VALUE));
    props.setProperty("venice.writer.max.size.for.user.payload.per.message.in.bytes", "972800");
    return props;
  }

  private Row createPutRow(String key, String value, long offset) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, offset, MessageType.PUT.getValue(), 1, key.getBytes(), value.getBytes(), 1,
            "rmd".getBytes() },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  private Row createDeleteRow(String key, long offset) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, offset, MessageType.DELETE.getValue(), 1, key.getBytes(), new byte[0], 1,
            "rmd".getBytes() },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  /**
   * Custom factory that creates test partition writers instead of real ones.
   */
  public static class TestSparkPartitionWriterFactory implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 1L;
    private final Broadcast<Properties> jobProps;
    private final DataWriterAccumulators accumulators;
    private final String testName;

    public TestSparkPartitionWriterFactory(
        String testName,
        Broadcast<Properties> jobProps,
        DataWriterAccumulators accumulators) {
      this.testName = testName;
      this.jobProps = jobProps;
      this.accumulators = accumulators;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> rows) throws Exception {
      try (TestSparkPartitionWriter partitionWriter =
          new TestSparkPartitionWriter(testName, jobProps.getValue(), accumulators)) {
        partitionWriter.processRows(rows);
      }
      return rows;
    }
  }

  /**
   * Test implementation that ONLY overrides getKafkaInputDataFrame() and createPartitionWriterFactory().
   * Everything else uses the real AbstractDataWriterSparkJob implementation.
   */
  private class TestDataWriterSparkJob extends DataWriterSparkJob {
    protected final String testName;

    TestDataWriterSparkJob(String testName) {
      this.testName = testName;
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      // ONLY mocked method - provides test data instead of reading from Kafka
      List<Row> mockRows =
          Arrays.asList(createPutRow("test-key-1", "test-value-1", 1L), createPutRow("test-key-2", "test-value-2", 2L));
      return getSparkSession().createDataFrame(mockRows, RAW_PUBSUB_INPUT_TABLE_SCHEMA);
    }

    @Override
    protected MapPartitionsFunction<Row, Row> createPartitionWriterFactory(
        Broadcast<Properties> broadcastProperties,
        DataWriterAccumulators accumulators) {
      // Override to return test factory that captures output
      return new TestSparkPartitionWriterFactory(testName, broadcastProperties, accumulators);
    }
  }

  private class TestDataWriterSparkJobWithDeletes extends TestDataWriterSparkJob {
    TestDataWriterSparkJobWithDeletes(String testName) {
      super(testName);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      List<Row> mockRows = Arrays.asList(createDeleteRow("delete-key", 200L));
      return getSparkSession().createDataFrame(mockRows, RAW_PUBSUB_INPUT_TABLE_SCHEMA);
    }
  }
}
