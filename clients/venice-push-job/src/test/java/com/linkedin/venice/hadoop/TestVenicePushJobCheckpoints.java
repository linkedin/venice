package com.linkedin.venice.hadoop;

import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.POLL_JOB_STATUS_INTERVAL_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.CounterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Time;
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
import org.apache.avro.Schema;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestVenicePushJobCheckpoints {
  private static final int PARTITION_COUNT = 10;
  private static final int NUMBER_OF_FILES_TO_READ_AND_BUILD_DICT_COUNT = 1; // DUMMY Number of files for
                                                                             // ValidateSchemaAndBuildDictMapper
  private static final String TEST_CLUSTER_NAME = "some-cluster";
  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_SCHEMA_STR = "\"string\"";

  private static final String SIMPLE_FILE_SCHEMA_STR = "{\n" + "    \"namespace\": \"example.avro\",\n"
      + "    \"type\": \"record\",\n" + "    \"name\": \"User\",\n" + "    \"fields\": [\n"
      + "      { \"name\": \"id\", \"type\": \"string\" },\n" + "      { \"name\": \"name\", \"type\": \"string\" },\n"
      + "      { \"name\": \"age\", \"type\": \"int\" },\n" + "      { \"name\": \"company\", \"type\": \"string\" }\n"
      + "    ]\n" + "  }";

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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.QUOTA_EXCEEDED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.QUOTA_EXCEEDED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "true");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  /**
   * Similar to {@link #testHandleQuotaExceeded}. Some counters and checkpoints changes here.
   */
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Storage quota exceeded.*", dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHandleQuotaExceededWithCompressionCollectionEnabled(boolean useMapperToBuildDict) throws Exception {
    List<PushJobCheckpoints> expectedCheckpoints;
    if (useMapperToBuildDict) {
      expectedCheckpoints = Arrays.asList(
          PushJobCheckpoints.INITIALIZE_PUSH_JOB,
          PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
          PushJobCheckpoints.NEW_VERSION_CREATED,
          PushJobCheckpoints.QUOTA_EXCEEDED);
    } else {
      expectedCheckpoints = Arrays.asList(
          PushJobCheckpoints.INITIALIZE_PUSH_JOB,
          PushJobCheckpoints.NEW_VERSION_CREATED,
          PushJobCheckpoints.QUOTA_EXCEEDED);
    }

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
        expectedCheckpoints,
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, String.valueOf(useMapperToBuildDict));
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
        });
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testWithCompressionCollectionEnabled(boolean useMapperToBuildDict) throws Exception {
    List<PushJobCheckpoints> expectedCheckpoints;
    if (useMapperToBuildDict) {
      expectedCheckpoints = Arrays.asList(
          PushJobCheckpoints.INITIALIZE_PUSH_JOB,
          PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
          PushJobCheckpoints.NEW_VERSION_CREATED,
          PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
          PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED);
    } else {
      expectedCheckpoints = Arrays.asList(
          PushJobCheckpoints.INITIALIZE_PUSH_JOB,
          PushJobCheckpoints.NEW_VERSION_CREATED,
          PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
          PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED);
    }

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
        expectedCheckpoints,
        properties -> {
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, String.valueOf(useMapperToBuildDict));
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
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
        Arrays.asList(PushJobCheckpoints.INITIALIZE_PUSH_JOB, PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
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
        Arrays.asList(PushJobCheckpoints.INITIALIZE_PUSH_JOB, PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.WRITE_ACL_FAILED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE),
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*partition writer closed count \\(0\\).*")
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.START_DATA_WRITER_JOB),
        10L, // Non-empty input data file
        properties -> {
          properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, "false");
          properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, "false");
        });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Task tracker is not reliable.*")
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.START_DATA_WRITER_JOB),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED // Expect the job to finish successfully
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
    // Input file size cannot be zero.
    new InputDataInfoProvider.InputDataInfo(0, 1, false, System.currentTimeMillis());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The Number of Input files is expected to be positive. Got: 0")
  public void testInitInputDataInfoWithIllegalNumInputFiles() {
    // Input file size cannot be zero.
    new InputDataInfoProvider.InputDataInfo(10L, 0, false, System.currentTimeMillis());
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*partition writer closed count \\(9\\).*")
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.START_DATA_WRITER_JOB),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
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
            PushJobCheckpoints.INITIALIZE_PUSH_JOB,
            PushJobCheckpoints.NEW_VERSION_CREATED,
            PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
            PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED),
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

    final List<PushJobCheckpoints> expectedReportedCheckpoints;
    if (useMapperToBuildDict) {
      /** Uses {@link ValidateSchemaAndBuildDictMapper} to validate schema and build dictionary which will checkpoint DATASET_CHANGED before NEW_VERSION_CREATED */
      expectedReportedCheckpoints =
          Arrays.asList(PushJobCheckpoints.INITIALIZE_PUSH_JOB, PushJobCheckpoints.DATASET_CHANGED);
    } else {
      /** {@link InputDataInfoProvider#validateInputAndGetInfo} in VPJ driver validates schema and build dictionary which will checkpoint NEW_VERSION_CREATED before DATASET_CHANGED.
       * DATASET_CHANGED will only be checked in the MR job to process data after creating the new version */
      expectedReportedCheckpoints = Arrays.asList(
          PushJobCheckpoints.INITIALIZE_PUSH_JOB,
          PushJobCheckpoints.NEW_VERSION_CREATED,
          PushJobCheckpoints.DATASET_CHANGED);
    }

    runJobAndAssertCheckpoints(jobClientWrapper, 10, 1, true, true, ExecutionStatus.COMPLETED, properties -> {
      properties.setProperty(COMPRESSION_METRIC_COLLECTION_ENABLED, String.valueOf(compressionMetricCollectionEnabled));
      properties.setProperty(USE_MAPPER_TO_BUILD_DICTIONARY, String.valueOf(useMapperToBuildDict));
    }, expectedReportedCheckpoints);
  }

  @DataProvider(name = "DvcErrorExecutionStatus")
  public static Object[][] dvcErrorExecutionStatus() {
    return allPermutationGenerator((permutation) -> {
      ExecutionStatus status = (ExecutionStatus) permutation[0];
      if (status.isDVCIngestionError()) {
        return true;
      }
      return false;
    }, ExecutionStatus.values());
  }

  @Test(expectedExceptions = { VeniceException.class }, dataProvider = "DvcErrorExecutionStatus")
  public void testHandleDVCFailureCheckpoints(ExecutionStatus status) throws Exception {
    JobClientWrapper jobClientWrapper = mock(JobClientWrapper.class);
    doAnswer(invocation -> null).when(jobClientWrapper).runJobWithConfig(any());

    final List<PushJobCheckpoints> expectedReportedCheckpoints;
    expectedReportedCheckpoints = Arrays.asList(
        PushJobCheckpoints.INITIALIZE_PUSH_JOB,
        PushJobCheckpoints.NEW_VERSION_CREATED,
        PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED,
        PushJobCheckpoints.valueOf(status.toString()));

    runJobAndAssertCheckpoints(
        jobClientWrapper,
        10,
        1,
        true,
        false,
        status,
        properties -> {},
        expectedReportedCheckpoints);
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<PushJobCheckpoints> expectedReportedCheckpoints,
      Consumer<Properties> extraProps) throws Exception {
    testHandleErrorsInCounter(mockCounterInfos, expectedReportedCheckpoints, 10L, extraProps);
  }

  private void testHandleErrorsInCounter(
      List<MockCounterInfo> mockCounterInfos,
      List<PushJobCheckpoints> expectedReportedCheckpoints,
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
      List<PushJobCheckpoints> expectedReportedCheckpoints,
      long inputFileDataSizeInBytes,
      int numInputFiles,
      boolean inputFileHasRecords,
      Consumer<Properties> extraProps) throws Exception {
    runJobAndAssertCheckpoints(
        createJobClientWrapperMock(mockCounterInfos),
        inputFileDataSizeInBytes,
        numInputFiles,
        inputFileHasRecords,
        false,
        ExecutionStatus.COMPLETED,
        extraProps,
        expectedReportedCheckpoints);
  }

  private void runJobAndAssertCheckpoints(
      JobClientWrapper jobClientWrapper,
      long inputFileDataSizeInBytes,
      int numInputFiles,
      boolean inputFileHasRecords,
      boolean datasetChanged,
      ExecutionStatus executionStatus,
      Consumer<Properties> extraProps,
      List<PushJobCheckpoints> expectedReportedCheckpoints) throws Exception {
    Properties props = getVPJProps();
    if (extraProps != null) {
      extraProps.accept(props);
    }
    ControllerClient controllerClient = mock(ControllerClient.class);
    configureControllerClientMock(controllerClient, props, executionStatus);
    configureClusterDiscoverControllerClient(controllerClient);
    try (VenicePushJob venicePushJob = new VenicePushJob("job-id", props)) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.setKmeSchemaSystemStoreControllerClient(controllerClient);
      venicePushJob.setJobClientWrapper(jobClientWrapper);
      if (executionStatus.isDVCIngestionError()) {
        DataWriterComputeJob dataWriterComputeJob = venicePushJob.getDataWriterComputeJob();
        DataWriterComputeJob spyDataWriterComputeJob = spy(dataWriterComputeJob);
        venicePushJob.setDataWriterComputeJob(spyDataWriterComputeJob);
        Counters counters = new Counters();
        DataWriterTaskTracker counterBackedMapReduceDataWriterTaskTracker =
            new CounterBackedMapReduceDataWriterTaskTracker(counters);
        when(spyDataWriterComputeJob.getTaskTracker()).thenReturn(counterBackedMapReduceDataWriterTaskTracker);
      }
      InputDataInfoProvider inputDataInfoProvider = getInputDataInfoProviderMock(
          props,
          venicePushJob.getPushJobSetting(),
          inputFileDataSizeInBytes,
          numInputFiles,
          inputFileHasRecords,
          datasetChanged);
      venicePushJob.setInputDataInfoProvider(inputDataInfoProvider);
      venicePushJob.setVeniceWriter(createVeniceWriterMock());
      SentPushJobDetailsTrackerImpl pushJobDetailsTracker = new SentPushJobDetailsTrackerImpl();
      venicePushJob.setSentPushJobDetailsTracker(pushJobDetailsTracker);
      venicePushJob
          .setValidateSchemaAndBuildDictMapperOutputReader(getValidateSchemaAndBuildDictMapperOutputReaderMock());

      try {
        venicePushJob.run();
      } finally {
        List<Integer> actualReportedCheckpointValues =
            new ArrayList<>(pushJobDetailsTracker.getRecordedPushJobDetails().size());

        for (PushJobDetails pushJobDetails: pushJobDetailsTracker.getRecordedPushJobDetails()) {
          actualReportedCheckpointValues.add(pushJobDetails.pushJobLatestCheckpoint);
        }
        List<Integer> expectedCheckpointValues =
            expectedReportedCheckpoints.stream().map(PushJobCheckpoints::getValue).collect(Collectors.toList());

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

    props.put(VENICE_DISCOVER_URL_PROP, "venice-urls");
    props.put(VENICE_STORE_NAME_PROP, "store-name");
    props.put(INPUT_PATH_PROP, "input-path");
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);
    props.put(POLL_JOB_STATUS_INTERVAL_MS, 1000);
    props.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "test");
    props.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "test");
    props.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, "true");
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
      Properties properties,
      PushJobSetting pushJobSetting,
      long inputFileDataSizeInBytes,
      int numInputFiles,
      boolean inputFileHasRecords,
      boolean datasetChanged) throws Exception {
    InputDataInfoProvider inputDataInfoProvider = mock(InputDataInfoProvider.class);
    InputDataInfoProvider.InputDataInfo inputDataInfo = new InputDataInfoProvider.InputDataInfo(
        inputFileDataSizeInBytes,
        numInputFiles,
        inputFileHasRecords,
        System.currentTimeMillis());
    when(inputDataInfoProvider.validateInputAndGetInfo(anyString())).thenAnswer(invocation -> {
      Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(SIMPLE_FILE_SCHEMA_STR);
      pushJobSetting.keyField = properties.getProperty(KEY_FIELD_PROP);
      pushJobSetting.valueField = properties.getProperty(VALUE_FIELD_PROP);

      pushJobSetting.inputDataSchema = schema;
      pushJobSetting.valueSchema = schema.getField(pushJobSetting.valueField).schema();

      pushJobSetting.inputDataSchemaString = SIMPLE_FILE_SCHEMA_STR;
      pushJobSetting.keySchema = pushJobSetting.inputDataSchema.getField(pushJobSetting.keyField).schema();

      pushJobSetting.keySchemaString = pushJobSetting.keySchema.toString();
      pushJobSetting.valueSchemaString = pushJobSetting.valueSchema.toString();

      return inputDataInfo;
    });

    if (datasetChanged) {
      when(inputDataInfoProvider.getInputLastModificationTime(anyString()))
          .thenReturn(System.currentTimeMillis() + 10 * Time.MS_PER_MINUTE);
    }

    doReturn(ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData()).when(inputDataInfoProvider)
        .trainZstdDictionary();

    return inputDataInfoProvider;
  }

  private JobStatusQueryResponse createJobStatusQueryResponseMock(ExecutionStatus executionStatus) {
    JobStatusQueryResponse jobStatusQueryResponse = mock(JobStatusQueryResponse.class);
    when(jobStatusQueryResponse.isError()).thenReturn(false);
    when(jobStatusQueryResponse.getExtraInfo()).thenReturn(Collections.emptyMap());
    if (executionStatus == ExecutionStatus.COMPLETED) {
      when(jobStatusQueryResponse.getStatus()).thenReturn(ExecutionStatus.COMPLETED.toString());
    } else {
      when(jobStatusQueryResponse.getStatus()).thenReturn(executionStatus.toString());
    }
    when(jobStatusQueryResponse.getOptionalStatusDetails()).thenReturn(Optional.empty());
    when(jobStatusQueryResponse.getOptionalExtraDetails()).thenReturn(Optional.empty());

    return jobStatusQueryResponse;
  }

  private void configureControllerClientMock(
      ControllerClient controllerClient,
      Properties props,
      ExecutionStatus executionStatus) {
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
    when(keySchemaResponse.getSchemaStr()).thenReturn(KEY_SCHEMA_STR);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getId()).thenReturn(1);
    when(valueSchemaResponse.getSchemaStr()).thenReturn(VALUE_SCHEMA_STR);

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
            anyBoolean(),
            any(),
            anyInt(),
            anyBoolean())).thenReturn(versionCreationResponse);
    JobStatusQueryResponse jobStatusQueryResponse = createJobStatusQueryResponseMock(executionStatus);
    when(controllerClient.queryOverallJobStatus(anyString(), any(), any())).thenReturn(jobStatusQueryResponse);

    doAnswer(invocation -> {
      return null;
    }).when(controllerClient).killOfflinePushJob(anyString());

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
    when(versionCreationResponse.getPartitionerClass()).thenReturn(DefaultVenicePartitioner.class.getCanonicalName());
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
