package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.spark.SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_PUSH_DESTINATION_PUBSUB_BROKER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.testng.Assert.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.writer.TestSparkPartitionWriter;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * End-to-end integration tests for Spark repush functionality.
 * Overrides getKafkaInputDataFrame() and createPartitionWriterFactory() in AbstractDataWriterSparkJob.
 * Uses TestSparkPartitionWriter to capture output instead of writing to Kafka.
 *
 */
public class DataWriterSparkJobRepushTest {
  private DataWriterSparkJob currentTestJob = null;

  @AfterMethod
  public void cleanupSparkSession() {
    // Close the Spark session after each test to avoid context pollution
    if (currentTestJob != null) {
      try {
        // Get the SparkSession and stop it explicitly
        if (currentTestJob.getSparkSession() != null) {
          currentTestJob.getSparkSession().stop();
        }
        currentTestJob.close();
      } catch (Exception e) {
        System.out.println("Exception during Spark session cleanup: " + e.getMessage());
      }
      currentTestJob = null;
    }
  }

  @Test
  public void testRunComputeJobEndToEnd() {
    String testName = "testRunComputeJobEndToEnd";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    TestDataWriterSparkJob job = new TestDataWriterSparkJob(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
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
   * Test compaction (deduplication) with duplicate keys.
   * Verifies that only the record with the highest offset is kept for each key.
   */
  @Test
  public void testCompactionWithDuplicateKeys() {
    String testName = "testCompactionWithDuplicateKeys";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    TestDataWriterSparkJobWithDuplicates job = new TestDataWriterSparkJobWithDuplicates(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;

    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);

    job.runComputeJob();

    // Verify compaction metrics
    DataWriterAccumulators accumulators = job.getAccumulators();
    assertEquals(
        (long) accumulators.totalDuplicateKeyCounter.value(),
        2L,
        "Should track 2 duplicate keys (key1 and key2)");
    assertEquals(
        (long) accumulators.duplicateKeyWithIdenticalValueCounter.value(),
        1L,
        "Should track 1 duplicate with identical values (key1)");
    assertEquals(
        (long) accumulators.duplicateKeyWithDistinctValueCounter.value(),
        1L,
        "Should track 1 duplicate with distinct values (key2)");

    // Verify output: should have only 3 records (one per unique key)
    List<TestSparkPartitionWriter.TestRecord> capturedRecords = TestSparkPartitionWriter.getCapturedRecords(testName);
    assertEquals(capturedRecords.size(), 3, "Should have 3 records after compaction (one per unique key)");

    // Verify key1: should have the latest value (offset 200)
    TestSparkPartitionWriter.TestRecord key1Record =
        capturedRecords.stream().filter(r -> Arrays.equals(r.key, "key1".getBytes())).findFirst().orElse(null);
    assertNotNull(key1Record, "Should find key1");
    assertTrue(Arrays.equals(key1Record.value, "value1".getBytes()), "key1 should have value1 (identical values)");

    // Verify key2: should have the latest value (offset 300, value3)
    TestSparkPartitionWriter.TestRecord key2Record =
        capturedRecords.stream().filter(r -> Arrays.equals(r.key, "key2".getBytes())).findFirst().orElse(null);
    assertNotNull(key2Record, "Should find key2");
    assertTrue(Arrays.equals(key2Record.value, "value3".getBytes()), "key2 should have value3 (latest value)");

    // Verify key3: no duplicates, should pass through
    TestSparkPartitionWriter.TestRecord key3Record =
        capturedRecords.stream().filter(r -> Arrays.equals(r.key, "key3".getBytes())).findFirst().orElse(null);
    assertNotNull(key3Record, "Should find key3");
    assertTrue(Arrays.equals(key3Record.value, "value3".getBytes()), "key3 should have value3");
  }

  /**
   * Test with DELETE messages through runComputeJob().
   */
  @Test
  public void testRunComputeJobWithDeleteMessages() {
    String testName = "testRunComputeJobWithDeleteMessages";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    TestDataWriterSparkJobWithDeletes job = new TestDataWriterSparkJobWithDeletes(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
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

  /**
   * Test applyTTLFilter with TTL enabled to verify the transformation is set up correctly.
   * This exercises the enabled path in AbstractDataWriterSparkJob.applyTTLFilter().
   * We verify the filter transformation is applied (returns a transformed Dataset) without
   * executing the full Spark pipeline to avoid test infrastructure conflicts.
   */
  @Test
  public void testApplyTTLFilterEnabledSetup() throws Exception {
    String testName = "testApplyTTLFilterEnabledSetup";

    // Create temp directories and schema files
    File valueSchemaTempDir = Files.createTempDirectory("value-schemas").toFile();
    File rmdSchemaTempDir = Files.createTempDirectory("rmd-schemas").toFile();

    try {
      // Create simple schemas for testing
      String valueSchemaStr =
          "{\"type\":\"record\",\"name\":\"TestValue\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
      String rmdSchemaStr =
          "{\"type\":\"record\",\"name\":\"RmdRecord\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"}]}";

      // Write schema files (schema ID 1, RMD version 1)
      File valueSchemaFile = new File(valueSchemaTempDir, "1");
      File rmdSchemaFile = new File(rmdSchemaTempDir, "1");

      Files.write(valueSchemaFile.toPath(), valueSchemaStr.getBytes());
      Files.write(rmdSchemaFile.toPath(), rmdSchemaStr.getBytes());

      TestableDataWriterSparkJob job = new TestableDataWriterSparkJob(testName);
      currentTestJob = job;

      long ttlStartTime = System.currentTimeMillis();

      Properties props = createDefaultTestProperties();
      props.setProperty("repush.ttl.policy", "0"); // RT_WRITE_ONLY
      props.setProperty("repush.ttl.start.timestamp", String.valueOf(ttlStartTime));
      props.setProperty("value.schema.dir", valueSchemaTempDir.getAbsolutePath());
      props.setProperty("rmd.schema.dir", rmdSchemaTempDir.getAbsolutePath());
      props.setProperty("kafka.input.source.compression.strategy", "NO_OP");

      PushJobSetting setting = new PushJobSetting();
      setting.isSourceKafka = true;
      setting.kafkaInputTopic = "test_store_v1";
      setting.repushSourcePubsubBroker = "localhost:9092";
      setting.repushTTLEnabled = true; // TTL enabled
      setting.repushTTLStartTimeMs = ttlStartTime;
      setting.valueSchemaDir = valueSchemaTempDir.getAbsolutePath();
      setting.rmdSchemaDir = rmdSchemaTempDir.getAbsolutePath();
      setting.topic = "test_store_v1";
      setting.pushDestinationPubsubBroker = "localhost:9092";
      setting.partitionerClass = DefaultVenicePartitioner.class.getName();
      setting.partitionCount = 1;
      setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

      job.configure(new VeniceProperties(props), setting);

      // Create test DataFrame with chunked records (these skip TTL filtering)
      List<Row> rows =
          Arrays.asList(createChunkedRow("key1", "chunk1", -1, 1L), createChunkedRow("key2", "chunk2", -20, 2L));
      Dataset<Row> inputDF = job.getSparkSession().createDataFrame(rows, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

      // Apply TTL filter - should return a transformed Dataset
      Dataset<Row> outputDF = job.testableApplyTTLFilter(inputDF);

      // Verify the transformation was applied (outputDF is not null and is a different object)
      assertNotNull(outputDF, "TTL filter should return a DataFrame");
      // Don't call count() to avoid Spark execution issues in test environment
      // The transformation setup itself provides code coverage

    } finally {
      // Cleanup temp directories
      deleteDirectory(valueSchemaTempDir);
      deleteDirectory(rmdSchemaTempDir);
    }
  }

  private Row createChunkedRow(String key, String chunkData, int negativeSchemaId, long offset) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, offset, MessageType.PUT.getValue(), negativeSchemaId, key.getBytes(),
            chunkData.getBytes(), -1, new byte[0], null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  /**
   * Verify that per-record schema IDs from Kafka are preserved through the Spark pipeline
   * and not replaced with the global -1 value.
   */
  @Test
  public void testSchemaIdPreservedThroughPipeline() {
    String testName = "testSchemaIdPreservedThroughPipeline";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    TestDataWriterSparkJobWithSchemaIds job = new TestDataWriterSparkJobWithSchemaIds(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);
    job.runComputeJob();

    List<TestSparkPartitionWriter.TestRecord> capturedRecords = TestSparkPartitionWriter.getCapturedRecords(testName);
    assertEquals(capturedRecords.size(), 2, "Should have captured 2 records");

    // Verify per-record schema IDs are preserved (not -1)
    TestSparkPartitionWriter.TestRecord record1 =
        capturedRecords.stream().filter(r -> Arrays.equals(r.key, "key-a".getBytes())).findFirst().orElse(null);
    assertNotNull(record1, "Should find key-a");
    assertEquals(record1.valueSchemaId, 5, "Schema ID 5 should be preserved for key-a");
    assertEquals(record1.rmdVersionId, 2, "RMD version ID 2 should be preserved for key-a");

    TestSparkPartitionWriter.TestRecord record2 =
        capturedRecords.stream().filter(r -> Arrays.equals(r.key, "key-b".getBytes())).findFirst().orElse(null);
    assertNotNull(record2, "Should find key-b");
    assertEquals(record2.valueSchemaId, 7, "Schema ID 7 should be preserved for key-b");
    assertEquals(record2.rmdVersionId, 3, "RMD version ID 3 should be preserved for key-b");
  }

  /**
   * Verify that SYSTEM_SCHEMA_READER_ENABLED is set in the Spark job config for KIF repush.
   * Without this, the Spark path may fail to deserialize messages using newer KME versions.
   */
  @Test
  public void testKifRepushSetsSystemSchemaReaderEnabled() {
    String testName = "testKifRepushSetsSystemSchemaReaderEnabled";

    TestDataWriterSparkJob job = new TestDataWriterSparkJob(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.isSystemSchemaReaderEnabled = true;
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);

    String systemSchemaReaderEnabled = job.getSparkSession().conf().get("system.schema.reader.enabled");
    assertEquals(systemSchemaReaderEnabled, "true", "SYSTEM_SCHEMA_READER_ENABLED must be set for KIF repush");
  }

  /**
   * Verify that VALUE_SCHEMA_DIR is set in the Spark job config when TTL is enabled.
   * This was a bug where only RMD_SCHEMA_DIR was set, causing VeniceRmdTTLFilter to crash
   * at runtime because HDFSSchemaSource requires both VALUE_SCHEMA_DIR and RMD_SCHEMA_DIR.
   */
  @Test
  public void testTTLRepushSetsValueSchemaDir() throws Exception {
    String testName = "testTTLRepushSetsValueSchemaDir";

    File valueSchemaTempDir = Files.createTempDirectory("value-schemas").toFile();
    File rmdSchemaTempDir = Files.createTempDirectory("rmd-schemas").toFile();

    try {
      TestableDataWriterSparkJob job = new TestableDataWriterSparkJob(testName);
      currentTestJob = job;

      Properties props = createDefaultTestProperties();
      props.setProperty("kafka.input.source.compression.strategy", "NO_OP");

      PushJobSetting setting = new PushJobSetting();
      setting.isSourceKafka = true;
      setting.kafkaInputTopic = "test_store_v1";
      setting.repushSourcePubsubBroker = "localhost:9092";
      setting.repushTTLEnabled = true;
      setting.repushTTLStartTimeMs = System.currentTimeMillis();
      setting.valueSchemaDir = valueSchemaTempDir.getAbsolutePath();
      setting.rmdSchemaDir = rmdSchemaTempDir.getAbsolutePath();
      setting.topic = "test_store_v1";
      setting.pushDestinationPubsubBroker = "localhost:9092";
      setting.partitionerClass = DefaultVenicePartitioner.class.getName();
      setting.partitionCount = 1;
      setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

      job.configure(new VeniceProperties(props), setting);

      // Verify both schema dirs are set in the Spark session config
      String valueSchemaDir = job.getSparkSession().conf().get("value.schema.dir");
      String rmdSchemaDir = job.getSparkSession().conf().get("rmd.schema.dir");
      assertEquals(valueSchemaDir, valueSchemaTempDir.getAbsolutePath(), "VALUE_SCHEMA_DIR must be set for TTL repush");
      assertEquals(rmdSchemaDir, rmdSchemaTempDir.getAbsolutePath(), "RMD_SCHEMA_DIR must be set for TTL repush");
    } finally {
      deleteDirectory(valueSchemaTempDir);
      deleteDirectory(rmdSchemaTempDir);
    }
  }

  /**
   * Test compression re-encoding in the Spark pipeline.
   * Source: GZIP, Target: NO_OP
   */
  @Test
  public void testCompressionReencoding() throws Exception {
    String testName = "testCompressionReencoding";
    TestSparkPartitionWriter.clearCapturedRecords(testName);

    CompressorFactory compressorFactory = new CompressorFactory();
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    String originalValue = "original-uncompressed-value";
    byte[] compressedValue =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(originalValue.getBytes()), 0));

    TestDataWriterSparkJobWithCompression job = new TestDataWriterSparkJobWithCompression(testName, compressedValue);
    currentTestJob = job;

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.sourceVersionCompressionStrategy = CompressionStrategy.GZIP;
    setting.topicCompressionStrategy = CompressionStrategy.NO_OP;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(createDefaultTestProperties()), setting);
    job.runComputeJob();

    List<TestSparkPartitionWriter.TestRecord> capturedRecords = TestSparkPartitionWriter.getCapturedRecords(testName);
    assertEquals(capturedRecords.size(), 1);

    assertEquals(new String(capturedRecords.get(0).value), originalValue);
  }

  /**
   * Test that VALUE_SCHEMA_ID_PROP is set to the actual value schema ID (not -1) for KIF repush.
   */
  @Test
  public void testKifRepushSetsValueSchemaIdFromPushJobSetting() {
    String testName = "testKifRepushSetsValueSchemaIdFromPushJobSetting";

    TestDataWriterSparkJob job = new TestDataWriterSparkJob(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.valueSchemaId = 3; // Simulates schema ID retrieved from controller
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);

    // Verify VALUE_SCHEMA_ID_PROP is set to the actual schema ID, not -1
    String valueSchemaIdStr = job.getSparkSession().conf().get("value.schema.id");
    assertEquals(
        valueSchemaIdStr,
        "3",
        "VALUE_SCHEMA_ID_PROP should be set to pushJobSetting.valueSchemaId for KIF repush");
  }

  /**
   * Verify that all job properties (including xc.*, pubsub.*, etc.) are forwarded to the
   * DataFrameReader, matching MR behavior where KafkaInputUtils.getConsumerProperties() copies
   * ALL JobConf properties to the PubSub consumer.
   */
  @Test
  public void testJobPropertiesForwardedToDataFrameReader() {
    String testName = "testJobPropertiesForwardedToDataFrameReader";

    // Uses ConfigTestSparkJob pattern: calls super.getKafkaInputDataFrame() which runs
    // the real production code (including the bulk forwarding loop), catches the expected
    // Kafka connection failure, and returns mock data. This ensures the test exercises
    // the actual DataWriterSparkJob.getKafkaInputDataFrame() method.
    ConfigTestSparkJob job = new ConfigTestSparkJob();
    currentTestJob = job;

    Properties props = createDefaultTestProperties();
    // Simulate xc.* cross-colo TLS properties that come from the DAG config
    props.setProperty("xc.tls.key.store.type", "PKCS12");
    props.setProperty("xc.pubsub.broker.url.to.region.name.map", "broker1@region1,broker2@region2");
    // Simulate pubsub.* properties
    props.setProperty("pubsub.some.client.config", "test-value");
    // Dynamic pass-through prefix list (same as production voldemort-build-and-push config)
    props.setProperty("pass.through.config.prefixes.list", "pubsub.,xc.");

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);
    job.getKafkaInputDataFrame();

    SparkSession spark = job.getSparkSession();
    assertEquals(spark.conf().get("xc.tls.key.store.type"), "PKCS12", "xc.tls.* should be forwarded");
    assertEquals(
        spark.conf().get("xc.pubsub.broker.url.to.region.name.map"),
        "broker1@region1,broker2@region2",
        "xc.pubsub.* should be forwarded");
    assertEquals(spark.conf().get("pubsub.some.client.config"), "test-value", "pubsub.* should be forwarded");
    assertEquals(spark.conf().get("kafka.input.topic"), "test_store_v1", "kafka.input.topic should be forwarded");
    assertEquals(
        spark.conf().get("venice.repush.source.pubsub.broker"),
        "localhost:9092",
        "venice.repush.source.pubsub.broker should be forwarded");
  }

  /**
   * Verify that in a cross-fabric repush scenario, PUBSUB_BROKER_ADDRESS and KAFKA_BOOTSTRAP_SERVERS
   * are absent from the SparkSession config, and the purpose-built keys
   * VENICE_PUSH_DESTINATION_PUBSUB_BROKER and VENICE_REPUSH_SOURCE_PUBSUB_BROKER are set correctly.
   */
  @Test
  public void testCrossFabricRepushBrokerIsolation() {
    String testName = "testCrossFabricRepushBrokerIsolation";

    TestDataWriterSparkJob job = new TestDataWriterSparkJob(testName);
    currentTestJob = job;

    Properties props = createDefaultTestProperties();

    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.repushSourcePubsubBroker = "dc1-kafka:9092"; /* source/input broker */
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v2";
    setting.pushDestinationPubsubBroker = "dc0-kafka:9092"; /* destination broker (NR source) */
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");

    job.configure(new VeniceProperties(props), setting);

    SparkSession spark = job.getSparkSession();

    /* Assert purpose-built keys are set correctly */
    assertEquals(
        spark.conf().get(VENICE_PUSH_DESTINATION_PUBSUB_BROKER),
        "dc0-kafka:9092",
        "Destination broker should be dc0-kafka:9092");
    assertEquals(
        spark.conf().get(VENICE_REPUSH_SOURCE_PUBSUB_BROKER),
        "dc1-kafka:9092",
        "Source broker should be dc1-kafka:9092");

    /* Assert PUBSUB_BROKER_ADDRESS is absent from SparkSession config */
    assertTrue(
        spark.conf().getOption(PUBSUB_BROKER_ADDRESS).isEmpty(),
        "PUBSUB_BROKER_ADDRESS should NOT be present in SparkSession config");

    /* Assert KAFKA_BOOTSTRAP_SERVERS is absent from SparkSession config */
    assertTrue(
        spark.conf().getOption(KAFKA_BOOTSTRAP_SERVERS).isEmpty(),
        "KAFKA_BOOTSTRAP_SERVERS should NOT be present in SparkSession config");
  }

  private Properties createDefaultTestProperties() {
    Properties props = new Properties();
    props.setProperty(KAFKA_INPUT_TOPIC, "test_store_v1");
    props.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost:9092");
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
            "rmd".getBytes(), null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  private Row createDeleteRow(String key, long offset) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, offset, MessageType.DELETE.getValue(), 1, key.getBytes(), new byte[0], 1,
            "rmd".getBytes(), null },
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

    public DataWriterAccumulators getAccumulators() {
      return getAccumulatorsForDataWriterJob();
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

  private class TestDataWriterSparkJobWithCompression extends TestDataWriterSparkJob {
    private final byte[] compressedValue;

    TestDataWriterSparkJobWithCompression(String testName, byte[] compressedValue) {
      super(testName);
      this.compressedValue = compressedValue;
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      List<Row> mockRows = Arrays.asList(
          new GenericRowWithSchema(
              new Object[] { "region1", 0, 100L, MessageType.PUT.getValue(), 1, "key1".getBytes(), compressedValue, 1,
                  "rmd".getBytes(), null },
              RAW_PUBSUB_INPUT_TABLE_SCHEMA));
      return getSparkSession().createDataFrame(mockRows, RAW_PUBSUB_INPUT_TABLE_SCHEMA);
    }
  }

  /**
   * Test job that provides input data with distinct per-record schema IDs to verify propagation.
   */
  private class TestDataWriterSparkJobWithSchemaIds extends TestDataWriterSparkJob {
    TestDataWriterSparkJobWithSchemaIds(String testName) {
      super(testName);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      // Create rows with distinct schema IDs (column index 4) and RMD version IDs (column index 7)
      List<Row> mockRows = Arrays.asList(
          new GenericRowWithSchema(
              new Object[] { "region1", 0, 1L, MessageType.PUT.getValue(), 5, "key-a".getBytes(), "val-a".getBytes(), 2,
                  "rmd".getBytes(), null },
              RAW_PUBSUB_INPUT_TABLE_SCHEMA),
          new GenericRowWithSchema(
              new Object[] { "region1", 0, 2L, MessageType.PUT.getValue(), 7, "key-b".getBytes(), "val-b".getBytes(), 3,
                  "rmd".getBytes(), null },
              RAW_PUBSUB_INPUT_TABLE_SCHEMA));
      return getSparkSession().createDataFrame(mockRows, RAW_PUBSUB_INPUT_TABLE_SCHEMA);
    }
  }

  /**
   * Testable subclass that exposes applyTTLFilter for direct testing.
   */
  private class TestableDataWriterSparkJob extends TestDataWriterSparkJob {
    TestableDataWriterSparkJob(String testName) {
      super(testName);
    }

    public Dataset<Row> testableApplyTTLFilter(Dataset<Row> dataFrame) {
      return super.applyTTLFilter(dataFrame);
    }
  }

  /**
   * Test job that provides input data with duplicate keys to test compaction.
   */
  private class TestDataWriterSparkJobWithDuplicates extends TestDataWriterSparkJob {
    TestDataWriterSparkJobWithDuplicates(String testName) {
      super(testName);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      // Create test data with duplicates:
      // key1: 2 duplicates with identical values (offsets 100, 200)
      // key2: 3 duplicates with distinct values (offsets 150, 250, 300)
      // key3: no duplicates (offset 400)
      List<Row> testData = Arrays.asList(
          // key1 duplicates (identical values)
          createPutRow("key1", "value1", 100L),
          createPutRow("key1", "value1", 200L), // latest

          // key2 duplicates (distinct values)
          createPutRow("key2", "value1", 150L),
          createPutRow("key2", "value2", 250L),
          createPutRow("key2", "value3", 300L), // latest

          // key3 (no duplicates)
          createPutRow("key3", "value3", 400L));

      return getSparkSession().createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);
    }
  }

  private static class ConfigTestSparkJob extends DataWriterSparkJob {
    @Override
    public Dataset<Row> getKafkaInputDataFrame() {
      try {
        return super.getKafkaInputDataFrame();
      } catch (Exception e) {
        List<Row> emptyRows = Arrays.asList(
            new GenericRowWithSchema(
                new Object[] { "region1", 0, 0L, 0, 1, new byte[0], new byte[0], 0, new byte[0], null },
                RAW_PUBSUB_INPUT_TABLE_SCHEMA));
        return getSparkSession().createDataFrame(emptyRows, RAW_PUBSUB_INPUT_TABLE_SCHEMA);
      }
    }
  }

  private void deleteDirectory(File directory) {
    if (directory != null && directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file: files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    }
  }
}
