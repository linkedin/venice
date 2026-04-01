package com.linkedin.venice.featurematrix.write;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.setup.ClusterConfigBuilder;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes batch push (VPJ) and incremental push operations using Venice's integration test utilities.
 */
public class BatchPushExecutor {
  private static final Logger LOGGER = LogManager.getLogger(BatchPushExecutor.class);

  /**
   * Writes test input data to the given directory.
   * - When chunking is enabled: pads NameRecordV1 string fields to exceed the chunk threshold.
   * - When TTL repush is enabled: adds a LONG "rmd" timestamp field so VPJ can attach RMD
   *   to each record, making them compatible with the subsequent KIF TTL repush.
   * - Default: plain NameRecordV1 records.
   *
   * @return the record schema
   */
  public static Schema writeTestInputData(File inputDir, TestCaseConfig config) throws IOException {
    if (config.isTtlRepush()) {
      // Build NameRecordV1 schema with an additional LONG "rmd" timestamp field.
      // VPJ will extract this field (via RMD_FIELD_PROP) and attach it as RMD on each
      // Kafka record so that the KIF TTL repush can filter records using VeniceRmdTTLFilter.
      Schema schemaWithTimestamp = new PushInputSchemaBuilder().setKeySchema(TestWriteUtils.STRING_SCHEMA)
          .setValueSchema(TestWriteUtils.NAME_RECORD_V1_SCHEMA)
          .setFieldSchema(VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP, Schema.create(Schema.Type.LONG))
          .build();
      long timestamp = System.currentTimeMillis();
      return TestWriteUtils.writeSimpleAvroFile(inputDir, schemaWithTimestamp, i -> {
        GenericRecord record = TestWriteUtils.renderNameRecord(schemaWithTimestamp, i);
        record.put(VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP, timestamp);
        return record;
      }, TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT);
    }
    if (config.isChunking()) {
      int padSize = ClusterConfigBuilder.CHUNKING_TEST_RECORD_SIZE / 2;
      char[] pad = new char[padSize];
      return TestWriteUtils.writeSimpleAvroFile(inputDir, TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA, i -> {
        GenericRecord record = TestWriteUtils.renderNameRecord(TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA, i);
        GenericRecord value = (GenericRecord) record.get(VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP);
        Arrays.fill(pad, (char) ('a' + (i % 26)));
        String suffix = new String(pad);
        value.put("firstName", value.get("firstName").toString() + suffix);
        value.put("lastName", value.get("lastName").toString() + suffix);
        return record;
      }, TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT);
    }
    return writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
  }

  /**
   * Executes a batch push using the Spark DataWriter engine.
   */
  public static void executeBatchPush(
      String storeName,
      TestCaseConfig config,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster,
      File inputDir) {
    LOGGER.info(
        "Executing batch push for store={}, compression={}, deferredSwap={}, targetRegion={}, ttlRepush={}",
        storeName,
        config.getCompression(),
        config.isDeferredSwap(),
        config.isTargetRegionPush(),
        config.isTtlRepush());

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProps = defaultVPJProps(multiRegionCluster, inputDirPath, storeName);
    vpjProps
        .setProperty(VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    vpjProps.setProperty(VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    if (config.isTtlRepush()) {
      // Tell VPJ to extract the "rmd" LONG field from each input record and use it as
      // the RMD timestamp. This makes records compatible with the subsequent TTL repush.
      vpjProps.setProperty(VenicePushJobConstants.RMD_FIELD_PROP, VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP);
    }

    if (config.isTargetRegionPush() && config.isDeferredSwap()) {
      // Combined flag: push to target region, then deferred swap service replicates and swaps
      vpjProps.setProperty(VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, "true");
    } else {
      if (config.isDeferredSwap()) {
        vpjProps.setProperty(VenicePushJobConstants.DEFER_VERSION_SWAP, "true");
      }
      if (config.isTargetRegionPush()) {
        vpjProps.setProperty(VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED, "true");
      }
    }

    IntegrationTestPushUtils.runVPJ(vpjProps);
    LOGGER.info("Batch push completed for store {}", storeName);
  }

  /**
   * Executes a KIF (Kafka Input Format) repush with TTL filtering enabled.
   * Must be called after an initial batch push has created version 1 with RMD-tagged records.
   * The repush reads from the existing version topic and filters out TTL-expired records.
   */
  public static void executeTtlRepush(
      String storeName,
      String clusterName,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster) {
    LOGGER.info("Executing TTL repush for store={}", storeName);

    String brokerUrl = multiRegionCluster.getChildRegions()
        .get(0)
        .getClusters()
        .get(clusterName)
        .getPubSubBrokerWrapper()
        .getAddress();

    Properties vpjProps = defaultVPJProps(multiRegionCluster, "dummyInputPath", storeName);
    vpjProps
        .setProperty(VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    vpjProps.setProperty(VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");
    vpjProps.setProperty(VenicePushJobConstants.SOURCE_KAFKA, "true");
    vpjProps.setProperty(VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER, brokerUrl);
    vpjProps.setProperty(VenicePushJobConstants.REPUSH_TTL_ENABLE, "true");

    IntegrationTestPushUtils.runVPJ(vpjProps);
    LOGGER.info("TTL repush completed for store {}", storeName);
  }

  /**
   * Executes an incremental push using VPJ with the incremental push flag.
   */
  public static void executeIncrementalPush(
      String storeName,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster,
      File inputDir) {
    LOGGER.info("Executing incremental push for store={}", storeName);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProps = defaultVPJProps(multiRegionCluster, inputDirPath, storeName);
    vpjProps
        .setProperty(VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    vpjProps.setProperty(VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");
    vpjProps.setProperty(VenicePushJobConstants.INCREMENTAL_PUSH, "true");

    IntegrationTestPushUtils.runVPJ(vpjProps);
    LOGGER.info("Incremental push completed for store {}", storeName);
  }
}
