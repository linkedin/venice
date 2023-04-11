package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_STRATEGY;
import static com.linkedin.venice.hadoop.VenicePushJob.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.hadoop.VenicePushJob.MULTI_REGION;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.hadoop.VenicePushJob.USE_MAPPER_TO_BUILD_DICTIONARY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVenicePushJobCheckpoints {
  private static final int PARTITION_COUNT = 10;
  private static final int NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT = 1; // DUMMY Number of files for
                                                                             // ValidateSchemaAndBuildDictMapper
  private static final String TEST_CLUSTER_NAME = "some-cluster";
  private static final String SCHEMA_STR = "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   "
      + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\" },  "
      + "       { \"name\": \"name\", \"type\": \"string\" },  " + "       { \"name\": \"age\", \"type\": \"int\" },  "
      + "       { \"name\": \"company\", \"type\": \"string\" }  " + "  ] " + " } ";
  private static final String SIMPLE_FILE_SCHEMA_STR = "{\n" + "    \"type\": \"record\",\n"
      + "    \"name\": \"Type1\",\n" + "    \"fields\": [\n" + "        {\n" + "            \"name\": \"something\",\n"
      + "            \"type\": \"string\"\n" + "        }\n" + "    ]\n" + "}";

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Storage quota exceeded.*")
  public void testHandleQuotaExceeded() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Quota exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1001),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.QUOTA_EXCEEDED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  /**
   * Similar to {@link #testHandleQuotaExceeded}. Some counters and checkpoints changes here.
   */
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Storage quota exceeded.*")
  public void testHandleQuotaExceededWithMapperToBuildDict() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Quota exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1001),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below:
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT + 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.QUOTA_EXCEEDED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  /**
   * Similar to {@link #testHandleQuotaExceeded}. Some counters and checkpoints changes here.
   */
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Storage quota exceeded.*")
  public void testHandleQuotaExceededWithCompressionCollectionEnabled() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Quota exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1001),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT + 1),
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.QUOTA_EXCEEDED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
        });
  }

  /**
   * Similar to {@link #testHandleQuotaExceededWithCompressionCollectionEnabled}, but USE_MAPPER_TO_BUILD_DICTIONARY to true.
   * It shouldn't make any difference as COMPRESSION_METRIC_COLLECTION_ENABLED being true will force enable USE_MAPPER_TO_BUILD_DICTIONARY too.
   */
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Storage quota exceeded.*")
  public void testHandleQuotaExceededWithCompressionCollectionEnabledV1() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Quota exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1001),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT + 1),
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.QUOTA_EXCEEDED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
        });
  }

  @Test
  public void testWithNoMapperToBuildDictionary() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  @Test
  public void testWithMapperToBuildDictionary() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT + 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  @Test
  public void testWithCompressionCollectionDisabled() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  @Test
  public void testWithCompressionCollectionEnabled() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT + 1),
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
        });
  }

  /**
   * Similar to {@link #testWithCompressionCollectionEnabled}, but USE_MAPPER_TO_BUILD_DICTIONARY to true.
   * It shouldn't make any difference as COMPRESSION_METRIC_COLLECTION_ENABLED being true will force enable USE_MAPPER_TO_BUILD_DICTIONARY too.
   */
  @Test
  public void testWithCompressionCollectionEnabledV1() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT + 1),
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
        });
  }

  /**
   * Handle cases where dictionary creation in the mapper failed: COMPRESSION_METRIC_COLLECTION_ENABLED is true,
   * but the compression strategy is {@link CompressionStrategy#NO_OP}, leading to the failure being ignored and
   * the job finishes all check points as the success case.
   */
  @Test
  public void testHandlingFailureWithCompressionCollectionEnabled() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT), // no +1 as the last part (build dict) failed
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
        });
  }

  /**
   * Handle cases where dictionary creation in the mapper failed: COMPRESSION_METRIC_COLLECTION_ENABLED is true,
   * and the compression strategy is {@link CompressionStrategy#ZSTD_WITH_DICT}, leading to the failure captured
   * via exception and checkpoints reflecting the same.
   */
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Training ZSTD compression dictionary failed.*")
  public void testHandlingFailureWithCompressionCollectionEnabledAndZstdCompression() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT), // no +1 as the last part (build dict) failed
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
          properties.setProperty(COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.toString());
        });
  }

  /**
   * Handle cases where dictionary creation in the mapper is skipped: COMPRESSION_METRIC_COLLECTION_ENABLED is true,
   * but the compression strategy is {@link CompressionStrategy#NO_OP}, leading to the skip being ignored and
   * the job finishes all check points as the success case.
   */
  @Test
  public void testHandlingSkippedWithCompressionCollectionEnabled() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT), // no +1 as the last part (build dict) failed
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
        });
  }

  /**
   * Handle cases where dictionary creation in the mapper skipped: COMPRESSION_METRIC_COLLECTION_ENABLED is true,
   * and the compression strategy is {@link CompressionStrategy#ZSTD_WITH_DICT}, leading to the skip captured
   * via exception and checkpoints reflecting the same.
   */
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Training ZSTD compression dictionary skipped.*")
  public void testHandlingSkippedWithCompressionCollectionEnabledAndZstdCompression() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT),
            // ValidateSchemaAndBuildDictMapper related counters below
            // Number of Processed files
            new MockCounterInfo(
                MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME,
                NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT), // no +1 as the last part (build dict) failed
            // Dictionary building succeeded if enabled
            new MockCounterInfo(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME, 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "true");
          properties.setProperty(COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.toString());
        });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Insufficient ACLs to write to the store")
  public void testHandleWriteAclFailed() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 1), // Write ACL failed
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.WRITE_ACL_FAILED),
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Input data has at least 1 keys that appear more than once.*")
  public void testHandleDuplicatedKeyWithDistinctValue() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // Duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 1),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE),
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*reducer job closed count \\(0\\).*")
  public void testHandleZeroClosedReducersFailure() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Spray all partitions gets triggered
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 1),
            // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            // No authorization error
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // No reducers at all closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, 0)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.START_MAP_REDUCE_JOB),
        10L, // Non-empty input data file
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "MR job counter is not reliable.*")
  public void testUnreliableMapReduceCounter() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Spray all partitions gets triggered
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 1),
            // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 0),
            // No authorization error
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // No reducers at all closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, 0)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.START_MAP_REDUCE_JOB),
        10L, // Non-empty input data file
        1,
        true,
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test
  public void testHandleZeroClosedReducersWithNoRecordInputDataFile() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // No quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 0),
            // No authorization error
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // No reducers at all closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, 0)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED // Expect the job to finish successfully
        ),
        10L,
        1,
        false, // Input data file has no record
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The input data file size is expected to be positive. Got: 0")
  public void testInitInputDataInfoWithIllegalSize() {
    PushJobSchemaInfo pushJobSchemaInfo = new PushJobSchemaInfo();
    // Input file size cannot be zero.
    new InputDataInfoProvider.InputDataInfo(pushJobSchemaInfo, 0, 1, false, System.currentTimeMillis());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The Number of Input files is expected to be positive. Got: 0")
  public void testInitInputDataInfoWithIllegalNumInputFiles() {
    PushJobSchemaInfo pushJobSchemaInfo = new PushJobSchemaInfo();
    // Input file size cannot be zero.
    new InputDataInfoProvider.InputDataInfo(pushJobSchemaInfo, 10L, 0, false, System.currentTimeMillis());
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*reducer job closed count \\(9\\).*")
  public void testHandleInsufficientClosedReducersFailure() throws Exception {
    testHandleErrorsInCounter(
        Arrays.asList(
            // Spray all partitions gets triggered
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 1),
            // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            // No authorization error
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // Some but not all reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT - 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.START_MAP_REDUCE_JOB),
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test
  public void testCounterValidationWhenSprayAllPartitionsNotTriggeredButWithMismatchedReducerCount() throws Exception { // Successful
                                                                                                                        // workflow
    testHandleErrorsInCounter(
        Arrays.asList(
            // Spray all partitions isn't triggered
            new MockCounterInfo(MRJobCounterHelper.MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, 0),
            // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            // No authorization error
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // Some but not all reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT - 1)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test
  public void testHandleNoErrorInCounters() throws Exception { // Successful workflow
    testHandleErrorsInCounter(
        Arrays.asList(
            // Quota not exceeded
            new MockCounterInfo(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, 1),
            // No authorization error
            new MockCounterInfo(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, 0),
            // No duplicated key with distinct value
            new MockCounterInfo(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, 0),
            // All reducers closed
            new MockCounterInfo(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, PARTITION_COUNT)),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED,
            VenicePushJob.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = {
      VeniceException.class }, dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHandleMRFailureAndDatasetChange(
      boolean compressionMetricCollectionEnabled,
      boolean useMapperToBuildDict) throws Exception {
    JobClientWrapper jobClientWrapper = mock(JobClientWrapper.class);
    when(jobClientWrapper.runJobWithConfig(any())).thenThrow(new IOException("Job failed!"));

    InputDataInfoProvider inputDataInfoProvider = getInputDataInfoProviderMock(10L, 1, true);
    when(inputDataInfoProvider.getInputLastModificationTime(anyString())).thenReturn(System.currentTimeMillis() + 10L);

    // Pair<useMapperToBuildDict, compressionMetricCollectionEnabled>
    Map<Pair<Boolean, Boolean>, List<VenicePushJob.PushJobCheckpoints>> expectedReportedCheckpoints = new HashMap<>();
    /** Uses {@link ValidateSchemaAndBuildDictMapper} to validate schema and build dictionary which will checkpoint DATASET_CHANGED before NEW_VERSION_CREATED */
    expectedReportedCheckpoints.put(
        new Pair(true, true),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.DATASET_CHANGED));

    /** Below case will be changed to (true,true) internally */
    expectedReportedCheckpoints.put(new Pair(false, true), expectedReportedCheckpoints.get(new Pair(true, true)));

    /** Uses {@link ValidateSchemaAndBuildDictMapper} to validate schema and build dictionary which will checkpoint DATASET_CHANGED before NEW_VERSION_CREATED */
    expectedReportedCheckpoints.put(new Pair(true, false), expectedReportedCheckpoints.get(new Pair(true, true)));

    /** {@link InputDataInfoProvider#validateInputAndGetInfo} in VPJ driver validates schema and build dictionary which will checkpoint NEW_VERSION_CREATED before DATASET_CHANGED.
     * DATASET_CHANGED will only be checked in the MR job to process data after creating the new version */
    expectedReportedCheckpoints.put(
        new Pair(false, false),
        Arrays.asList(
            VenicePushJob.PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            VenicePushJob.PushJobCheckpoints.NEW_VERSION_CREATED,
            VenicePushJob.PushJobCheckpoints.DATASET_CHANGED));

    runJobAndAssertCheckpoints(jobClientWrapper, inputDataInfoProvider, properties -> {
      properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, String.valueOf(compressionMetricCollectionEnabled));
      properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, String.valueOf(useMapperToBuildDict));
    }, expectedReportedCheckpoints.get(new Pair(useMapperToBuildDict, compressionMetricCollectionEnabled)));
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints,
      Consumer<Properties> extraProps) throws Exception {
    testHandleErrorsInCounter(mockCounterInfos, expectedReportedCheckpoints, 10L, extraProps);
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints,
      long inputFileDataSizeInBytes,
      Consumer<Properties> extraProps) throws Exception {
    testHandleErrorsInCounter(
        mockCounterInfos,
        expectedReportedCheckpoints,
        inputFileDataSizeInBytes,
        NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT,
        inputFileDataSizeInBytes > 0,
        extraProps);
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints,
      long inputFileDataSizeInBytes,
      int numInputFiles,
      boolean inputFileHasRecords,
      Consumer<Properties> extraProps) throws Exception {
    runJobAndAssertCheckpoints(
        createJobClientWrapperMock(mockCounterInfos),
        getInputDataInfoProviderMock(inputFileDataSizeInBytes, numInputFiles, inputFileHasRecords),
        extraProps,
        expectedReportedCheckpoints);
  }

  private void runJobAndAssertCheckpoints(
      JobClientWrapper jobClientWrapper,
      InputDataInfoProvider inputDataInfoProvider,
      Consumer<Properties> extraProps,
      List<VenicePushJob.PushJobCheckpoints> expectedReportedCheckpoints) {

    Properties props = getVPJProps();
    if (extraProps != null) {
      extraProps.accept(props);
    }
    ControllerClient controllerClient = mock(ControllerClient.class);
    configureControllerClientMock(controllerClient, props);
    configureClusterDiscoverControllerClient(controllerClient);
    try (VenicePushJob venicePushJob = new VenicePushJob("job-id", props)) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.setKmeSchemaSystemStoreControllerClient(controllerClient);
      venicePushJob.setJobClientWrapper(jobClientWrapper);
      venicePushJob.setInputDataInfoProvider(inputDataInfoProvider);
      venicePushJob.setVeniceWriter(createVeniceWriterMock());
      SentPushJobDetailsTrackerImpl pushJobDetailsTracker = new SentPushJobDetailsTrackerImpl();
      venicePushJob.setSentPushJobDetailsTracker(pushJobDetailsTracker);
      try {
        venicePushJob
            .setValidateSchemaAndBuildDictMapperOutputReader(getValidateSchemaAndBuildDictMapperOutputReaderMock());
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        venicePushJob.run();
      } finally {
        List<Integer> actualReportedCheckpointValues =
            new ArrayList<>(pushJobDetailsTracker.getRecordedPushJobDetails().size());

        for (PushJobDetails pushJobDetails: pushJobDetailsTracker.getRecordedPushJobDetails()) {
          actualReportedCheckpointValues.add(pushJobDetails.pushJobLatestCheckpoint);
        }
        List<Integer> expectedCheckpointValues = expectedReportedCheckpoints.stream()
            .map(VenicePushJob.PushJobCheckpoints::getValue)
            .collect(Collectors.toList());

        Assert.assertEquals(actualReportedCheckpointValues, expectedCheckpointValues);
      }
    }
  }

  private Properties getVPJProps() {
    Properties props = new Properties();
    props.setProperty(MULTI_REGION, "false");

    String childRegion = "child_region";
    props.setProperty(SOURCE_GRID_FABRIC, childRegion);
    props.setProperty(D2_ZK_HOSTS_PREFIX + childRegion, "child.zk.com:1234");

    props.put(VenicePushJob.VENICE_DISCOVER_URL_PROP, "venice-urls");
    props.put(VenicePushJob.VENICE_STORE_NAME_PROP, "store-name");
    props.put(VenicePushJob.INPUT_PATH_PROP, "input-path");
    props.put(VenicePushJob.KEY_FIELD_PROP, "id");
    props.put(VenicePushJob.VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);
    props.put(VenicePushJob.POLL_JOB_STATUS_INTERVAL_MS, 1000);
    props.setProperty(VenicePushJob.SSL_KEY_STORE_PROPERTY_NAME, "test");
    props.setProperty(VenicePushJob.SSL_TRUST_STORE_PROPERTY_NAME, "test");
    props.setProperty(VenicePushJob.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(VenicePushJob.SSL_KEY_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(VenicePushJob.PUSH_JOB_STATUS_UPLOAD_ENABLE, "true");
    return props;
  }

  private ValidateSchemaAndBuildDictMapperOutputReader getValidateSchemaAndBuildDictMapperOutputReaderMock() {
    ValidateSchemaAndBuildDictMapperOutputReader validateSchemaAndBuildDictMapperOutputReader =
        mock(ValidateSchemaAndBuildDictMapperOutputReader.class);
    ValidateSchemaAndBuildDictMapperOutput output =
        new ValidateSchemaAndBuildDictMapperOutput(10L, ByteBuffer.wrap("Test".getBytes()));
    when(validateSchemaAndBuildDictMapperOutputReader.getOutput()).thenReturn(output);
    return validateSchemaAndBuildDictMapperOutputReader;
  }

  private InputDataInfoProvider getInputDataInfoProviderMock(
      long inputFileDataSizeInBytes,
      int numInputFiles,
      boolean inputFileHasRecords) throws Exception {
    InputDataInfoProvider inputDataInfoProvider = mock(InputDataInfoProvider.class);
    PushJobSchemaInfo pushJobSchemaInfo = new PushJobSchemaInfo();
    pushJobSchemaInfo.setKeySchemaString(SCHEMA_STR);
    pushJobSchemaInfo.setValueSchemaString(SCHEMA_STR);
    pushJobSchemaInfo.setKeyField("key-field");
    pushJobSchemaInfo.setValueField("value-field");
    pushJobSchemaInfo.setFileSchemaString(SIMPLE_FILE_SCHEMA_STR);
    InputDataInfoProvider.InputDataInfo inputDataInfo = new InputDataInfoProvider.InputDataInfo(
        pushJobSchemaInfo,
        inputFileDataSizeInBytes,
        numInputFiles,
        inputFileHasRecords,
        System.currentTimeMillis());
    when(inputDataInfoProvider.validateInputAndGetInfo(anyString())).thenReturn(inputDataInfo);
    return inputDataInfoProvider;
  }

  private JobStatusQueryResponse createJobStatusQueryResponseMock() {
    JobStatusQueryResponse jobStatusQueryResponse = mock(JobStatusQueryResponse.class);
    when(jobStatusQueryResponse.isError()).thenReturn(false);
    when(jobStatusQueryResponse.getExtraInfo()).thenReturn(Collections.emptyMap());
    when(jobStatusQueryResponse.getOptionalStatusDetails()).thenReturn(Optional.empty());
    when(jobStatusQueryResponse.getOptionalExtraDetails()).thenReturn(Optional.empty());
    when(jobStatusQueryResponse.getStatus()).thenReturn(ExecutionStatus.COMPLETED.toString());
    return jobStatusQueryResponse;
  }

  private void configureControllerClientMock(ControllerClient controllerClient, Properties props) {
    StoreResponse storeResponse = mock(StoreResponse.class);
    when(controllerClient.getClusterName()).thenReturn(TEST_CLUSTER_NAME);

    StoreInfo storeInfo = mock(StoreInfo.class);
    when(controllerClient.getValueSchema(anyString(), anyInt())).thenReturn(mock(SchemaResponse.class));

    StorageEngineOverheadRatioResponse storageEngineOverheadRatioResponse =
        mock(StorageEngineOverheadRatioResponse.class);
    when(storageEngineOverheadRatioResponse.isError()).thenReturn(false);
    when(storageEngineOverheadRatioResponse.getStorageEngineOverheadRatio()).thenReturn(1.0);

    when(storeInfo.getStorageQuotaInByte()).thenReturn(1000L);
    when(storeInfo.isSchemaAutoRegisterFromPushJobEnabled()).thenReturn(false);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(storeInfo.getCompressionStrategy()).thenReturn(
        CompressionStrategy.valueOf(props.getProperty(COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString())));

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(SCHEMA_STR);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getId()).thenReturn(12345);
    VersionCreationResponse versionCreationResponse = createVersionCreationResponse();

    when(controllerClient.getStore(anyString())).thenReturn(storeResponse);
    when(controllerClient.getStorageEngineOverheadRatio(anyString())).thenReturn(storageEngineOverheadRatioResponse);
    when(controllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);
    when(controllerClient.getValueSchemaID(anyString(), anyString())).thenReturn(valueSchemaResponse);
    when(
        controllerClient.requestTopicForWrites(
            anyString(),
            anyLong(),
            any(),
            anyString(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            any(),
            any(),
            any(),
            anyBoolean(),
            anyLong(),
            anyBoolean())).thenReturn(versionCreationResponse);
    JobStatusQueryResponse jobStatusQueryResponse = createJobStatusQueryResponseMock();
    when(controllerClient.queryOverallJobStatus(anyString(), any())).thenReturn(jobStatusQueryResponse);

    ControllerResponse controllerResponse = mock(ControllerResponse.class);
    when(controllerResponse.isError()).thenReturn(false);
    when(controllerClient.sendPushJobDetails(anyString(), anyInt(), any(byte[].class))).thenReturn(controllerResponse);
  }

  private void configureClusterDiscoverControllerClient(ControllerClient controllerClient) {
    D2ServiceDiscoveryResponse clusterDiscoveryResponse = mock(D2ServiceDiscoveryResponse.class);
    when(clusterDiscoveryResponse.isError()).thenReturn(false);
    when(clusterDiscoveryResponse.getCluster()).thenReturn(TEST_CLUSTER_NAME);
    when(controllerClient.discoverCluster(anyString())).thenReturn(clusterDiscoveryResponse);
  }

  private VersionCreationResponse createVersionCreationResponse() {
    VersionCreationResponse versionCreationResponse = mock(VersionCreationResponse.class);
    when(versionCreationResponse.isError()).thenReturn(false);
    when(versionCreationResponse.getKafkaTopic()).thenReturn("kafka-topic");
    when(versionCreationResponse.getVersion()).thenReturn(1);
    when(versionCreationResponse.getKafkaBootstrapServers()).thenReturn("kafka-bootstrap-server");
    when(versionCreationResponse.getPartitions()).thenReturn(PARTITION_COUNT);
    when(versionCreationResponse.isEnableSSL()).thenReturn(false);
    when(versionCreationResponse.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(versionCreationResponse.isDaVinciPushStatusStoreEnabled()).thenReturn(false);
    when(versionCreationResponse.getPartitionerClass()).thenReturn("PartitionerClass");
    when(versionCreationResponse.getPartitionerParams()).thenReturn(Collections.emptyMap());
    return versionCreationResponse;
  }

  private JobClientWrapper createJobClientWrapperMock(List<MockCounterInfo> mockCounterInfos) throws IOException {
    RunningJob runningJob = mock(RunningJob.class);
    Counters counters = mock(Counters.class);

    Map<String, Counters.Group> groupMap = new HashMap<>();
    for (MockCounterInfo mockCounterInfo: mockCounterInfos) {
      Counters.Group group = groupMap.computeIfAbsent(mockCounterInfo.getGroupName(), k -> mock(Counters.Group.class));
      when(group.getCounter(mockCounterInfo.getCounterName())).thenReturn(mockCounterInfo.getCounterValue());
      when(counters.getGroup(mockCounterInfo.getGroupName())).thenReturn(group);
    }
    when(runningJob.getCounters()).thenReturn(counters);
    JobID jobID = mock(JobID.class);
    when(jobID.toString()).thenReturn("temp");
    when(runningJob.getID()).thenReturn(jobID);
    JobClientWrapper jobClientWrapper = mock(JobClientWrapper.class);
    when(jobClientWrapper.runJobWithConfig(any())).thenReturn(runningJob);
    return jobClientWrapper;
  }

  private VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriterMock() {
    return mock(VeniceWriter.class);
  }

  private static class MockCounterInfo {
    private final MRJobCounterHelper.GroupAndCounterNames groupAndCounterNames;
    private final long counterValue;

    MockCounterInfo(MRJobCounterHelper.GroupAndCounterNames groupAndCounterNames, long counterValue) {
      this.groupAndCounterNames = groupAndCounterNames;
      this.counterValue = counterValue;
    }

    String getGroupName() {
      return groupAndCounterNames.getGroupName();
    }

    String getCounterName() {
      return groupAndCounterNames.getCounterName();
    }

    long getCounterValue() {
      return counterValue;
    }
  }
}
