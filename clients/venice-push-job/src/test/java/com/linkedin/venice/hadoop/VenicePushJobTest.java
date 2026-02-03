package com.linkedin.venice.hadoop;

import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.hadoop.VenicePushJob.getExecutionStatusFromControllerResponse;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_UPDATE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_REGULAR_PUSH_WITH_TTL_REPUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPLIANCE_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.LEGACY_AVRO_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.LEGACY_AVRO_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_TIMEOUT_OVERRIDE_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_SECONDS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_ETL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.ConcurrentBatchPushException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceStoreAclException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaMismatchException;
import com.linkedin.venice.hadoop.exceptions.VeniceValidationException;
import com.linkedin.venice.hadoop.mapreduce.datawriter.jobs.DataWriterMRJob;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */
public class VenicePushJobTest {
  private static final String TEST_PUSH = "test_push";
  private static final String TEST_URL = "test_url";
  private static final String TEST_PATH = "test_path";
  private static final String TEST_STORE = "test_store";
  private static final String TEST_CLUSTER = "test_cluster";
  private static final String TEST_SERVICE = "test_venice";
  private static final int REPUSH_VERSION = 1;

  private static final String TEST_PARENT_ZK_ADDRESS = "localhost:2180";
  private static final String TEST_ZK_ADDRESS = "localhost:2181";
  private static final String TEST_PARENT_CONTROLLER_D2_SERVICE = "ParentController";
  private static final String TEST_CHILD_CONTROLLER_D2_SERVICE = "ChildController";

  private static final String PUSH_JOB_ID = "push_job_number_101";
  private static final String DISCOVERY_URL = "d2://d2Clusters/venice-discovery";
  private static final String PARENT_REGION_NAME = "dc-parent";

  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_SCHEMA_STR = "\"string\"";

  private static final String RMD_SCHEMA_STR = "{\"type\":\"record\",\"name\":\"__replication_metadata\","
      + "\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"_venice_replication_checkpoint_vector\","
      + "\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[]},{\"name\":\"_venice_timestamp\","
      + "\"type\":\"long\",\"default\":0}]}";

  private static final String RMD_SCHEMA_STR_V2 = "{\"type\":\"record\",\"name\":\"__replication_metadata\","
      + "\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"_venice_replication_checkpoint_vector\","
      + "\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[]},{\"name\":\"_venice_timestamp\","
      + "\"type\":\"long\",\"default\":0},{\"name\":\"_venice_extra_field\",\"type\":\"string\",\"default\":\"\"}]}";

  private static final String SIMPLE_FILE_SCHEMA_STR = "{\n" + "    \"namespace\": \"example.avro\",\n"
      + "    \"type\": \"record\",\n" + "    \"name\": \"User\",\n" + "    \"fields\": [\n"
      + "      { \"name\": \"id\", \"type\": \"string\" },\n" + "      { \"name\": \"name\", \"type\": \"string\" },\n"
      + "      { \"name\": \"age\", \"type\": \"int\" },\n" + "      { \"name\": \"company\", \"type\": \"string\" }\n"
      + "    ]\n" + "  }";

  @Test
  public void testCheckLastModifiedTimestamp() throws Exception {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputUri = "file://" + inputDir.getAbsolutePath();

    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.inputURI = inputUri;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
    pushJobSetting.isZstdDictCreationRequired = false;
    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props);
        VenicePushJob mockJob = getSpyVenicePushJob(new Properties(), null)) {
      InputDataInfoProvider.InputDataInfo inputDataInfo = provider.validateInputAndGetInfo(inputUri);

      when(mockJob.getInputDataInfo()).thenReturn(inputDataInfo);
      when(mockJob.getInputDataInfo()).thenReturn(inputDataInfo);
      when(mockJob.getPushJobSetting()).thenReturn(pushJobSetting);
      when(mockJob.getPushJobSetting()).thenReturn(pushJobSetting);
      when(mockJob.getInputDataInfoProvider()).thenReturn(provider);
      when(mockJob.getInputDataInfoProvider()).thenReturn(provider);

      // No modifications to input and no interactions expected with mockJob
      mockJob.checkLastModificationTimeAndLog();
      verify(mockJob, never()).updatePushJobDetailsWithCheckpoint(any(PushJobCheckpoints.class));

      // Write a new file to the input directory
      TestWriteUtils.writeSimpleAvroFileWithStringToStringWithExtraSchema(inputDir);
      mockJob.checkLastModificationTimeAndLog();
      verify(mockJob, times(1)).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATASET_CHANGED);
    }
  }

  @Test
  public void testHandleVersionCreationACLError() {
    VenicePushJob mockJob = getSpyVenicePushJob(new Properties(), null);
    Throwable error = new VeniceStoreAclException("ACL error");
    VersionCreationResponse response = new VersionCreationResponse();
    response.setError(error);
    mockJob.handleVersionCreationError(response);
    verify(mockJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
  }

  @Test
  public void testHandleVersionCreationConcurrentPushError() {
    VenicePushJob mockJob = getSpyVenicePushJob(new Properties(), null);
    Throwable error = new ConcurrentBatchPushException("Another push is in progress");
    VersionCreationResponse response = new VersionCreationResponse();
    response.setError(error);
    mockJob.handleVersionCreationError(response);
    verify(mockJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.CONCURRENT_BATCH_PUSH);
  }

  @Test
  public void testVPJCheckInputUpdateSchema() {
    VenicePushJob vpj = mock(VenicePushJob.class);
    when(vpj.isUpdateSchema(anyString())).thenCallRealMethod();
    Assert.assertTrue(vpj.isUpdateSchema(NAME_RECORD_V1_UPDATE_SCHEMA.toString()));
    Assert.assertFalse(vpj.isUpdateSchema(NAME_RECORD_V1_SCHEMA.toString()));
  }

  @Test(expectedExceptions = VeniceSchemaMismatchException.class)
  public void testValidateKeySchemaMismatch() {
    String keySchema = "\"string\"";
    String serverKeySchema = "\"int\"";
    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), null);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeKeySchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(serverKeySchema);
    setting.keySchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(keySchema);
    vpj.validateKeySchema(vpj.getPushJobSetting());
  }

  @Test
  public void testValidateAndRetrieveRmdSchemaWithNoRmdField() {
    PushJobSetting setting = new PushJobSetting();
    setting.storeName = TEST_STORE;
    ControllerClient mockClient = mock(ControllerClient.class);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
    verifyNoInteractions(mockClient);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testValidateAndRetrieveRmdSchemaWithErrorResponseFromController() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    when(mockResponse.isError()).thenReturn(true);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
  }

  @Test
  public void testValidateAndRetrieveRmdSchema() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.valueSchemaId = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchema = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchema.getRmdValueSchemaId()).thenReturn(1);
    when(rmdSchema.getId()).thenReturn(1);
    when(rmdSchema.getSchemaStr()).thenReturn(RMD_SCHEMA_STR);
    when(mockResponse.getSchemas()).thenReturn(new MultiSchemaResponse.Schema[] { rmdSchema });
    when(mockResponse.isError()).thenReturn(false);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
    Assert.assertEquals(setting.replicationMetadataSchemaString, RMD_SCHEMA_STR);
    Assert.assertEquals(setting.rmdSchemaId, 1);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Cannot continue with push with RMD since the RMD schema for the value.*")
  public void testValidateAndRetrieveRmdSchemaWithMultipleRmdSchemas() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.valueSchemaId = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaV1 = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchemaV1.getRmdValueSchemaId()).thenReturn(1);
    when(rmdSchemaV1.getId()).thenReturn(1);
    when(rmdSchemaV1.getSchemaStr()).thenReturn(RMD_SCHEMA_STR);

    MultiSchemaResponse.Schema rmdSchemaV2 = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchemaV2.getRmdValueSchemaId()).thenReturn(1);
    when(rmdSchemaV2.getId()).thenReturn(2);
    when(rmdSchemaV2.getSchemaStr()).thenReturn(RMD_SCHEMA_STR_V2);

    when(mockResponse.getSchemas()).thenReturn(new MultiSchemaResponse.Schema[] { rmdSchemaV1, rmdSchemaV2 });
    when(mockResponse.isError()).thenReturn(false);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testValidateAndRetrieveRmdSchemaWithNoRmdSchemaForValueSchema() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.valueSchemaId = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchema = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchema.getRmdValueSchemaId()).thenReturn(2);
    when(rmdSchema.getId()).thenReturn(1);
    when(rmdSchema.getSchemaStr()).thenReturn(RMD_SCHEMA_STR);
    when(mockResponse.getSchemas()).thenReturn(new MultiSchemaResponse.Schema[] { rmdSchema });
    when(mockResponse.isError()).thenReturn(false);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush with TTL is only supported while using Kafka Input Format.*")
  public void testRepushTTLJobWithNonKafkaInput() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps, null)) {
      pushJob.run();
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush TTL is only supported for real-time only store.*")
  public void testRepushTTLJobWithBatchStore() {
    Properties repushProps = getRepushWithTTLProps();

    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          // the initial version for all regions is 1, otherwise the topic name would mismatch
          put("dc-0", 1);
          put("dc-1", 1);
        }
      });
    });
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps, client)) {
      pushJob.run();
    }
  }

  @Test
  public void testRepushTTLJobConfig() {
    // Test with default configs
    Properties repushProps1 = getRepushWithTTLProps();
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps1, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.repushTTLEnabled);
      Assert.assertEquals(pushJobSetting.repushTTLStartTimeMs, -1);
      Assert.assertTrue(Version.isPushIdTTLRePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Test with explicit TTL start timestamp
    Properties repushProps2 = getRepushWithTTLProps();
    repushProps2.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps2.setProperty(REPUSH_TTL_START_TIMESTAMP, "100");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps2, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.repushTTLEnabled);
      Assert.assertEquals(pushJobSetting.repushTTLStartTimeMs, 100);
      Assert.assertTrue(Version.isPushIdTTLRePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Test with explicit TTL age
    Properties repushProps3 = getRepushWithTTLProps();
    repushProps3.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps3.setProperty(REPUSH_TTL_SECONDS, "100");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps3, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.repushTTLEnabled);
      Assert.assertTrue(
          pushJobSetting.repushTTLStartTimeMs > 0
              && pushJobSetting.repushTTLStartTimeMs <= System.currentTimeMillis() - 100 * Time.MS_PER_SECOND);
    }

    // Test with both explicit TTL age and TTL start timestamp - Not allowed
    Properties repushProps4 = getRepushWithTTLProps();
    repushProps4.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps4.setProperty(REPUSH_TTL_START_TIMESTAMP, "100");
    repushProps4.setProperty(REPUSH_TTL_SECONDS, "100");
    VeniceException exception =
        Assert.expectThrows(VeniceException.class, () -> getSpyVenicePushJob(repushProps4, null));
    Assert.assertTrue(exception.getMessage().endsWith("Please set only one."));

    // Test with TTL disabled.
    Properties repushProps5 = getRepushWithTTLProps();
    repushProps5.setProperty(REPUSH_TTL_ENABLE, "false");
    // Doesn't matter if these are set, they should be ignored.
    repushProps5.setProperty(REPUSH_TTL_START_TIMESTAMP, "100");
    repushProps5.setProperty(REPUSH_TTL_SECONDS, "100");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps5, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertFalse(pushJobSetting.repushTTLEnabled);
      Assert.assertEquals(pushJobSetting.repushTTLStartTimeMs, -1);
      Assert.assertTrue(Version.isPushIdRePush(pushJob.getPushJobDetails().getPushId().toString()));
    }
  }

  @Test
  public void testCompliancePushJobConfig() {
    // Test with compliance push enabled
    Properties complianceProps = new Properties();
    complianceProps.setProperty(COMPLIANCE_PUSH, "true");
    try (VenicePushJob pushJob = getSpyVenicePushJob(complianceProps, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.isCompliancePush);
      Assert.assertTrue(Version.isPushIdCompliancePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Test with compliance push disabled (default)
    Properties regularProps = new Properties();
    try (VenicePushJob pushJob = getSpyVenicePushJob(regularProps, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertFalse(pushJobSetting.isCompliancePush);
      Assert.assertFalse(Version.isPushIdCompliancePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Compliance push cannot be combined with TTL repush
    Properties complianceRepushProps = getRepushWithTTLProps();
    complianceRepushProps.setProperty(COMPLIANCE_PUSH, "true");
    VeniceException e =
        Assert.expectThrows(VeniceException.class, () -> getSpyVenicePushJob(complianceRepushProps, null));
    Assert.assertTrue(e.getMessage().contains("Compliance push cannot be combined with TTL repush settings"));

    // Compliance push cannot be combined with regular push with TTL repush
    Properties complianceRegularTTLProps = new Properties();
    complianceRegularTTLProps.setProperty(COMPLIANCE_PUSH, "true");
    complianceRegularTTLProps.setProperty(ALLOW_REGULAR_PUSH_WITH_TTL_REPUSH, "true");
    VeniceException e2 =
        Assert.expectThrows(VeniceException.class, () -> getSpyVenicePushJob(complianceRegularTTLProps, null));
    Assert.assertTrue(e2.getMessage().contains("Compliance push cannot be combined with TTL repush settings"));
  }

  @Test
  public void testPushJobSettingWithD2Routing() {
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null));
    });
    try (VenicePushJob pushJob = getSpyVenicePushJobWithD2Routing(new Properties(), client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.d2Routing);
      Assert.assertEquals(pushJobSetting.controllerD2ServiceName, TEST_CHILD_CONTROLLER_D2_SERVICE);
      Assert.assertEquals(pushJobSetting.childControllerRegionD2ZkHosts, TEST_ZK_ADDRESS);
    }

    try (VenicePushJob multiRegionPushJob = getSpyVenicePushJobWithMultiRegionD2Routing(new Properties(), client)) {
      PushJobSetting multiRegionPushJobSetting = multiRegionPushJob.getPushJobSetting();
      Assert.assertTrue(multiRegionPushJobSetting.d2Routing);
      Assert.assertEquals(multiRegionPushJobSetting.controllerD2ServiceName, TEST_PARENT_CONTROLLER_D2_SERVICE);
      Assert.assertEquals(multiRegionPushJobSetting.parentControllerRegionD2ZkHosts, TEST_PARENT_ZK_ADDRESS);
    }
  }

  @Test
  public void testPushJobSettingWithLivenessHeartbeat() {
    Properties vpjProps = new Properties();
    vpjProps.setProperty(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "true");
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null));
    });
    try (VenicePushJob pushJob = getSpyVenicePushJob(vpjProps, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.livenessHeartbeatEnabled);
    }
  }

  @DataProvider(name = "DataWriterJobClasses")
  public Object[][] getDataWriterJobClasses() {
    return new Object[][] { { DataWriterMRJob.class }, { DataWriterSparkJob.class } };
  }

  /**
   * Test that VenicePushJob.cancel() is called after bootstrapToOnlineTimeoutInHours is reached.
   * UNKNOWN status is returned for pollStatusUntilComplete() to stall the job until cancel() can be called.
   */
  @Test(dataProvider = "DataWriterJobClasses")
  public void testPushJobTimeout(Class<? extends DataWriterComputeJob> dataWriterJobClass) throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(DATA_WRITER_COMPUTE_JOB_CLASS, dataWriterJobClass.getCanonicalName());
    props.put(PUSH_JOB_TIMEOUT_OVERRIDE_MS, 2);
    ControllerClient client = getClient();
    JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
    doReturn("UNKNOWN").when(response).getStatus();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), any(), anyBoolean());
    doReturn(response).when(client).killOfflinePushJob(anyString());

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      pushJobSetting.jobStatusInUnknownStateTimeoutMs = 100; // give some time for the timeout to run on the executor
      StoreInfo storeInfo = new StoreInfo();
      storeInfo.setBootstrapToOnlineTimeoutInHours(0);
      pushJobSetting.storeResponse = new StoreResponse();
      pushJobSetting.storeResponse.setStore(storeInfo);
      skipVPJValidation(pushJob);
      try {
        DataWriterComputeJob dataWriterJob = spy(pushJob.getDataWriterComputeJob());
        doNothing().when(dataWriterJob).configure(any(), any()); // the spark job takes a long time to configure
        pushJob.setDataWriterComputeJob(dataWriterJob);
        pushJob.run();
        fail("Test should fail because pollStatusUntilComplete() never saw COMPLETE status, but doesn't.");
      } catch (VeniceException e) {
        Assert.assertTrue(e.getMessage().contains("push job is still in unknown state."), e.getMessage());
      }
      verify(pushJob, times(1)).cancel();
      verify(pushJob.getDataWriterComputeJob(), times(1)).kill();
    }
  }

  /**
   * Ensures that the data writer job is killed if the job times out. Uses an Answer to stall the data writer job
   * while it's running in order for it to get killed properly.
   */
  @Test(dataProvider = "DataWriterJobClasses")
  public void testDataWriterComputeJobTimeout(Class<? extends DataWriterComputeJob> dataWriterJobClass)
      throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(DATA_WRITER_COMPUTE_JOB_CLASS, dataWriterJobClass.getCanonicalName());
    props.put(PUSH_JOB_TIMEOUT_OVERRIDE_MS, 5L);
    ControllerClient client = getClient();
    JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
    doReturn("SUCCESS").when(response).getStatus();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), any(), anyBoolean());
    doReturn(response).when(client).killOfflinePushJob(anyString());

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      StoreInfo storeInfo = new StoreInfo();
      storeInfo.setBootstrapToOnlineTimeoutInHours(1);
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      pushJobSetting.storeResponse = new StoreResponse();
      pushJobSetting.storeResponse.setStore(storeInfo);

      CountDownLatch runningJobLatch = new CountDownLatch(1);
      CountDownLatch killedJobLatch = new CountDownLatch(1);

      /*
       * 1. Data writer job starts and status is set to RUNNING.
       * 2. Timeout thread kills the data writer job and status is set to KILLED.
       * The latch is used to stall the runComputeJob() method until the data writer job is killed.
       */
      Answer<Void> stallDataWriterJob = invocation -> {
        // At this point, the data writer job status is already set to RUNNING.
        runningJobLatch.countDown(); // frees VenicePushJob.killJob()
        if (!killedJobLatch.await(10, TimeUnit.SECONDS)) { // waits for this data writer job to be killed
          fail("Timed out waiting for the data writer job to be killed.");
        }
        throw new VeniceException("No data found at source path");
      };

      Answer<Void> killDataWriterJob = invocation -> {
        if (!runningJobLatch.await(10, TimeUnit.SECONDS)) { // waits for job status to be set to RUNNING
          fail("Timed out waiting for the data writer job status to be set to RUNNING");
        }
        pushJob.killDataWriterJob(); // sets job status to KILLED
        killedJobLatch.countDown(); // frees DataWriterComputeJob.runComputeJob()
        return null;
      };

      DataWriterComputeJob dataWriterJob = spy(pushJob.getDataWriterComputeJob());
      try {
        skipVPJValidation(pushJob);
        doCallRealMethod().when(pushJob).runJobAndUpdateStatus();
        pushJob.setDataWriterComputeJob(dataWriterJob);
        doNothing().when(dataWriterJob).validateJob();
        doNothing().when(dataWriterJob).configure(any(), any()); // the spark job takes a long time to configure
        doAnswer(stallDataWriterJob).when(dataWriterJob).runComputeJob();
        doAnswer(killDataWriterJob).when(pushJob).killJob(any(), any());
        pushJob.run(); // data writer job will run in this main test thread
      } catch (VeniceException e) {
        // Expected, because the data writer job is not configured to run successfully in this unit test environment
      }
      assertEquals(runningJobLatch.getCount(), 0); // killDataWriterJob() does not occur in the main test thread
      assertEquals(killedJobLatch.getCount(), 0);
      verify(pushJob, times(1)).cancel();
      verify(dataWriterJob, times(1)).kill();
      assertEquals(pushJob.getDataWriterComputeJob().getStatus(), DataWriterComputeJob.Status.KILLED);
    }
  }

  private Properties getRepushWithTTLProps() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps.setProperty(SOURCE_KAFKA, "true");
    repushProps.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, REPUSH_VERSION));
    repushProps.setProperty(KAFKA_INPUT_BROKER_URL, "localhost");
    repushProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    return repushProps;
  }

  private VenicePushJob getSpyVenicePushJob(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJProps(TEST_URL, TEST_PATH, TEST_STORE, Collections.emptyMap());
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  private VenicePushJob getSpyVenicePushJobWithD2Routing(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJPropsWithD2Routing(
        null,
        null,
        Collections.singletonMap("dc-0", TEST_ZK_ADDRESS),
        null,
        TEST_CHILD_CONTROLLER_D2_SERVICE,
        TEST_PATH,
        TEST_STORE,
        Collections.emptyMap());
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  private VenicePushJob getSpyVenicePushJobWithMultiRegionD2Routing(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJPropsWithD2Routing(
        "dc-parent",
        TEST_PARENT_ZK_ADDRESS,
        Collections.singletonMap("dc-0", TEST_ZK_ADDRESS),
        TEST_PARENT_CONTROLLER_D2_SERVICE,
        TEST_CHILD_CONTROLLER_D2_SERVICE,
        TEST_PATH,
        TEST_STORE,
        Collections.emptyMap());
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  private VenicePushJob getSpyVenicePushJobInternal(
      Properties baseVPJProps,
      Properties props,
      ControllerClient client) {
    // for mocked tests, only attempt once.
    baseVPJProps.put(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);
    baseVPJProps.putAll(props);
    ControllerClient mockClient = client == null ? getClient() : client;
    VenicePushJob pushJob = spy(new VenicePushJob(TEST_PUSH, baseVPJProps));
    pushJob.setControllerClient(mockClient);
    pushJob.setKmeSchemaSystemStoreControllerClient(mockClient);
    return pushJob;
  }

  private ControllerClient getClient() {
    return getClient(storeInfo -> {}, false);
  }

  private ControllerClient getClient(Consumer<StoreInfo> storeInfo) {
    return getClient(storeInfo, false);
  }

  private ControllerClient getClient(Consumer<StoreInfo> storeInfo, boolean applyFirst) {
    ControllerClient client = mock(ControllerClient.class);
    doReturn(TEST_CLUSTER).when(client).getClusterName();
    // mock discover cluster
    D2ServiceDiscoveryResponse clusterResponse = new D2ServiceDiscoveryResponse();
    clusterResponse.setCluster(TEST_CLUSTER);
    clusterResponse.setD2Service(TEST_SERVICE);
    doReturn(clusterResponse).when(client).discoverCluster(TEST_STORE);

    // mock value schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(VALUE_SCHEMA_STR);
    doReturn(schemaResponse).when(client).getValueSchema(anyString(), anyInt());

    // mock fetching KME schema
    MultiSchemaResponse multiSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema valueSchema = mock(MultiSchemaResponse.Schema.class);
    when(valueSchema.getId()).thenReturn(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    when(valueSchema.getSchemaStr())
        .thenReturn(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString());
    when(multiSchemaResponse.getSchemas())
        .thenReturn(Collections.singletonList(valueSchema).toArray(new MultiSchemaResponse.Schema[0]));
    doReturn(multiSchemaResponse).when(client).getAllValueSchema(anyString());

    // mock storeinfo response
    StoreResponse storeResponse1 = new StoreResponse();
    storeResponse1.setStore(getStoreInfo(storeInfo, applyFirst));

    // simulating a version bump in a targeted colo push
    StoreResponse storeResponse2 = new StoreResponse();
    storeResponse2.setStore(getStoreInfo(info -> {
      info.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          put("dc-0", 1);
          put("dc-1", 0);
        }
      });

      storeInfo.accept(info);
    }, applyFirst));
    doReturn(storeResponse1).doReturn(storeResponse2).when(client).getStore(TEST_STORE);

    // mock key schema
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setId(1);
    keySchemaResponse.setSchemaStr(KEY_SCHEMA_STR);
    doReturn(keySchemaResponse).when(client).getKeySchema(TEST_STORE);

    // mock version creation
    mockVersionCreationResponse(client);

    ControllerResponse response = new ControllerResponse();
    doReturn(response).when(client).writeEndOfPush(anyString(), anyInt());
    doReturn(response).when(client).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    return client;
  }

  /**
   * Create a StoreInfo and associated Version for testing
   * @param info, supplemented StoreInfo that should be added in the result.
   * @param applyFirst, if true, apply the StoreInfo first before creating Version, otherwise apply the default values only.
   *                    This is useful for testing different Version objects returned by the StoreInfo.
   * @return
   */
  private StoreInfo getStoreInfo(Consumer<StoreInfo> info, boolean applyFirst) {
    StoreInfo storeInfo = new StoreInfo();

    storeInfo.setCurrentVersion(REPUSH_VERSION);
    storeInfo.setIncrementalPushEnabled(false);
    storeInfo.setStorageQuotaInByte(1L);
    storeInfo.setSchemaAutoRegisterFromPushJobEnabled(false);
    storeInfo.setChunkingEnabled(false);
    storeInfo.setCompressionStrategy(CompressionStrategy.NO_OP);
    storeInfo.setWriteComputationEnabled(false);
    storeInfo.setIncrementalPushEnabled(false);
    storeInfo.setNativeReplicationSourceFabric("dc-0");
    Map<String, Integer> coloMaps = new HashMap<String, Integer>() {
      {
        put("dc-0", 0);
        put("dc-1", 0);
      }
    };
    storeInfo.setColoToCurrentVersions(coloMaps);
    if (applyFirst) {
      info.accept(storeInfo);
    }
    if (storeInfo.getVersions() == null) {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      version.setHybridStoreConfig(storeInfo.getHybridStoreConfig());
      storeInfo.setVersions(Collections.singletonList(version));
    }
    if (!applyFirst) {
      info.accept(storeInfo);
    }
    return storeInfo;
  }

  private InputDataInfoProvider getMockInputDataInfoProvider() throws Exception {
    InputDataInfoProvider provider = mock(InputDataInfoProvider.class);
    // Input file size cannot be zero.
    InputDataInfoProvider.InputDataInfo info =
        new InputDataInfoProvider.InputDataInfo(10L, 1, false, System.currentTimeMillis());
    doReturn(info).when(provider).validateInputAndGetInfo(anyString());
    return provider;
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testVenicePushJobThrowsNpeIfVpjPropertiesIsNull() {
    new VenicePushJob(PUSH_JOB_ID, null);
  }

  @Test
  public void testGetPushJobSettingThrowsUndefinedPropertyException() {
    Properties props = getVpjRequiredProperties();
    Set<Object> reqPropKeys = props.keySet();
    for (Object prop: reqPropKeys) {
      Properties propsCopy = (Properties) props.clone();
      propsCopy.remove(prop);
      try {
        new VenicePushJob(PUSH_JOB_ID, propsCopy);
        fail("Should throw UndefinedPropertyException for missing property: " + prop);
      } catch (UndefinedPropertyException expected) {
      }
    }
  }

  private Properties getVpjRequiredProperties() {
    Properties props = new Properties();
    props.put(INPUT_PATH_PROP, TEST_PATH);
    props.put(VENICE_DISCOVER_URL_PROP, DISCOVERY_URL);
    props.put(MULTI_REGION, true);
    props.put(PARENT_CONTROLLER_REGION_NAME, PARENT_REGION_NAME);
    props.put(D2_ZK_HOSTS_PREFIX + PARENT_REGION_NAME, TEST_PARENT_ZK_ADDRESS);
    props.put(VENICE_STORE_NAME_PROP, TEST_STORE);
    return props;
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Duplicate key field config found.*")
  public void testVenicePushJobCanHandleLegacyFieldsThrowsExceptionIfDuplicateKeysButValuesDiffer() {
    Properties props = getVpjRequiredProperties();
    props.put(LEGACY_AVRO_KEY_FIELD_PROP, "id");
    props.put(KEY_FIELD_PROP, "name");
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test
  public void testVenicePushJobCanHandleLegacyFields() {
    Properties props = getVpjRequiredProperties();
    props.put(LEGACY_AVRO_KEY_FIELD_PROP, "id");
    props.put(LEGACY_AVRO_VALUE_FIELD_PROP, "name");
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      VeniceProperties veniceProperties = vpj.getJobProperties();
      assertNotNull(veniceProperties);
      assertEquals(veniceProperties.getString(KEY_FIELD_PROP), "id");
      assertEquals(veniceProperties.getString(VALUE_FIELD_PROP), "name");
    }
  }

  @Test
  public void testGetPushJobSetting() {
    Properties props = getVpjRequiredProperties();
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      assertNotNull(pushJobSetting);
      assertTrue(pushJobSetting.d2Routing);
    }
  }

  @Test
  public void testGetPushJobSettingShouldNotUseD2RoutingIfControllerUrlDoesNotStartWithD2() {
    Properties props = getVpjRequiredProperties();
    props.put(VENICE_DISCOVER_URL_PROP, "http://venice.db:9898");
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      assertNotNull(pushJobSetting);
      assertFalse(pushJobSetting.d2Routing);
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Incremental push is not supported while using Kafka Input Format")
  public void testGetPushJobSettingShouldThrowExceptionIfSourceIsKafkaAndJobIsIncPush() {
    Properties props = getVpjRequiredProperties();
    props.put(SOURCE_KAFKA, true);
    props.put(INCREMENTAL_PUSH, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Source ETL is not supported while using Kafka Input Format")
  public void testGetPushJobSettingShouldThrowExceptionWhenBothSourceKafkaAndEtlAreSet() {
    Properties props = getVpjRequiredProperties();
    props.put(SOURCE_KAFKA, true);
    props.put(SOURCE_ETL, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test
  public void testSendPushJobDetailsStatusWithTerminallyFailedStatus() {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    ControllerClient client = getClient();

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      // Set up initial state and basic metadata
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.STARTED);
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";

      // Send terminal status: ERROR
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.ERROR);
      pushJob.sendPushJobDetailsToController(); // Should send this
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Attempt to send another terminal status: KILLED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.KILLED);
      pushJob.sendPushJobDetailsToController(); // Should be skipped (duplicate terminal status)
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Call cancel; should not send again since last status was terminal
      pushJob.cancel();
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Send non-terminal status: COMPLETED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.COMPLETED);
      pushJob.sendPushJobDetailsToController(); // Should send this
      verify(client, times(2)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Call cancel again; last status was non-terminal, so this should send KILLED
      pushJob.cancel();
      verify(client, times(3)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testSendNonTerminalThenTerminalStatus() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.STARTED);
      pushJob.sendPushJobDetailsToController();
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.COMPLETED);
      pushJob.sendPushJobDetailsToController();
      verify(client, times(2)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testSendStatusAgainIfDifferentTerminalStatus() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.STARTED);
      pushJob.sendPushJobDetailsToController(); // sends STARTED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.COMPLETED);
      pushJob.sendPushJobDetailsToController(); // sends COMPLETED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.KILLED);
      pushJob.sendPushJobDetailsToController(); // should send again because COMPLETED != KILLED
      verify(client, times(3)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testSkipRepeatedFailedStatus() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.ERROR);
      pushJob.sendPushJobDetailsToController(); // first send
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.ERROR);
      pushJob.sendPushJobDetailsToController(); // should skip
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testHandleExceptionDuringSendPushDetailsToController() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = mock(ControllerClient.class);
    when(client.sendPushJobDetails(anyString(), anyInt(), any(byte[].class))).thenThrow(new RuntimeException("fake"));

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.overallStatus = null;
      pushJobDetails.clusterName = null;
      pushJob.sendPushJobDetailsToController(); // expected to throw an exception during serialization of push job
                                                // details
      verify(client, never()).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testShouldBuildZstdCompressionDictionary(
      boolean compressionMetricCollectionEnabled,
      boolean isIncrementalPush,
      boolean inputFileHasRecords) {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.compressionMetricCollectionEnabled = compressionMetricCollectionEnabled;
    pushJobSetting.isIncrementalPush = isIncrementalPush;

    for (CompressionStrategy compressionStrategy: CompressionStrategy.values()) {
      pushJobSetting.storeCompressionStrategy = compressionStrategy;

      if (isIncrementalPush) {
        assertFalse(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      } else if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        assertTrue(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      } else if (compressionMetricCollectionEnabled && inputFileHasRecords) {
        assertTrue(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      } else {
        assertFalse(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      }
    }
  }

  @Test
  public void testEvaluateCompressionMetricCollectionEnabled() {
    PushJobSetting pushJobSetting = new PushJobSetting();

    // Test with compressionMetricCollectionEnabled == false
    pushJobSetting.compressionMetricCollectionEnabled = false;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // reset settings for the below tests
    pushJobSetting.compressionMetricCollectionEnabled = true;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // Test with isIncrementalPush == true
    pushJobSetting.isIncrementalPush = true;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // reset settings for the below tests
    pushJobSetting.isIncrementalPush = false;
    assertTrue(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

  }

  @Test
  public void testTargetedRegionPushConfigValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(TARGETED_REGION_PUSH_ENABLED, false);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Targeted region push list is only supported when targeted region push is enabled");
    }

    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(INCREMENTAL_PUSH, true);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Incremental push is not supported while using targeted region push mode");
    }
  }

  @Test
  public void testTargetedRegionPushConfigOverride() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    // when targeted region push is enabled, but store doesn't have source fabric set.
    ControllerClient client = getClient(store -> {
      store.setNativeReplicationSourceFabric("");
    });
    JobStatusQueryResponse response = mockJobStatusQuery();

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      skipVPJValidation(pushJob);
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "The store either does not have native replication mode enabled or set up default source fabric."));
      }
    }

    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      Assert.assertEquals(pushJob.getPushJobSetting().targetedRegions, "dc-0");
    }
  }

  @Test
  public void testTargetedRegionPushReporting() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = response.getExtraInfo();
      // one of the regions failed, so should fail
      extraInfo.put("dc-0", ExecutionStatus.NOT_STARTED.toString());
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(e.getMessage().contains("Push job error"));
      }
    }

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = response.getExtraInfo();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
      extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
      // both regions completed, so should succeed

      ControllerResponse dataRecoveryResponse = new ControllerResponse();
      doReturn(dataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());
      pushJob.run();
    }
  }

  @Test
  public void testKMEValidationFailure() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    ControllerClient client = getClient();
    // mock fetching KME schema and intentionally return an older version
    MultiSchemaResponse multiSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema valueSchema = mock(MultiSchemaResponse.Schema.class);
    when(valueSchema.getId()).thenReturn(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion() - 1);
    when(valueSchema.getSchemaStr())
        .thenReturn(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString());
    when(multiSchemaResponse.getSchemas())
        .thenReturn(Collections.singletonList(valueSchema).toArray(new MultiSchemaResponse.Schema[0]));
    doReturn(multiSchemaResponse).when(client).getAllValueSchema(anyString());
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(
            e.getMessage().contains("KME protocol is upgraded in the push job but not in the Venice controller"));
      }
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationConsumptionForBatchStore() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      VersionCreationResponse mockVersionCreationResponse = mockVersionCreationResponse(client);
      mockVersionCreationResponse.setKafkaSourceRegion(null);

      // verify the kafka source region must be present when kick off post-validation consumption
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(
            e.getMessage().contains("Post-validation consumption halted due to no available source region found"));
      }
      mockVersionCreationResponse.setKafkaSourceRegion("dc-0");
      verify(pushJob, times(1)).postPushValidation();
    }

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);
      ControllerResponse badDataRecoveryResponse = new ControllerResponse();
      badDataRecoveryResponse.setError("error");
      doReturn(badDataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());
      // verify failure of data recovery will fail the push job
      try {
        pushJob.run();
      } catch (VeniceException e) {
        assertTrue(e.getMessage().contains("Can't push data for region"));
      }
    }

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      ControllerResponse goodDataRecoveryResponse = new ControllerResponse();
      doReturn(goodDataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());

      // the job should succeed
      pushJob.run();
    }
  }

  @Test
  public void testConfigValidateForHybridStore() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(10, 10, 10, null));
    }, true);
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);
      PushJobSetting setting = pushJob.getPushJobSetting();
      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      doCallRealMethod().when(pushJob).run();
      setting.suppressEndOfPushMessage = true;
      pushJob.run();
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationFailedForValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      mockVersionCreationResponse(client);

      doThrow(new VeniceValidationException("error")).when(pushJob).postPushValidation();

      assertThrows(VeniceValidationException.class, pushJob::run);
      verify(pushJob, never()).postValidationConsumption(any());
    }
  }

  @Test
  public void getExecutionStatusFromControllerResponseTest() {
    // some valid cases
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setStatus(ExecutionStatus.COMPLETED.toString());
    assertEquals(getExecutionStatusFromControllerResponse(response), ExecutionStatus.COMPLETED);
    response.setStatus(ExecutionStatus.ERROR.toString());
    assertEquals(getExecutionStatusFromControllerResponse(response), ExecutionStatus.ERROR);
    response.setStatus(ExecutionStatus.DVC_INGESTION_ERROR_OTHER.toString());
    assertEquals(getExecutionStatusFromControllerResponse(response), ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    // invalid case
    response.setStatus("INVALID_STATUS");
    VeniceException exception =
        Assert.expectThrows(VeniceException.class, () -> getExecutionStatusFromControllerResponse(response));
    Assert.assertTrue(
        exception.getMessage().contains("Invalid ExecutionStatus returned from backend. status: INVALID_STATUS"));

    Map<String, String> extraDetails = new HashMap<>();
    extraDetails.put("extraDetails", "invalid status");
    response.setExtraDetails(extraDetails);
    exception = Assert.expectThrows(VeniceException.class, () -> getExecutionStatusFromControllerResponse(response));
    Assert.assertTrue(
        exception.getMessage()
            .contains(
                "Invalid ExecutionStatus returned from backend. status: INVALID_STATUS, extra details: {extraDetails=invalid status}"));
  }

  @Test
  public void testGetPerColoPushJobDetailsStatusFromExecutionStatus() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      PushJobDetailsStatus pushJobDetailsStatus =
          VenicePushJob.getPerColoPushJobDetailsStatusFromExecutionStatus(status);
      switch (status) {
        case NOT_CREATED:
        case NEW:
        case NOT_STARTED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.NOT_CREATED);
          break;
        case STARTED:
        case PROGRESS:
        case CATCH_UP_BASE_TOPIC_OFFSET_LAG:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.STARTED);
          break;
        case END_OF_PUSH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.END_OF_PUSH_RECEIVED);
          break;
        case TOPIC_SWITCH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.DATA_WRITER_COMPLETED);
          break;
        case START_OF_INCREMENTAL_PUSH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
          break;
        case END_OF_INCREMENTAL_PUSH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
          break;
        case COMPLETED:
        case DATA_RECOVERY_COMPLETED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.COMPLETED);
          break;
        case ERROR:
        case DVC_INGESTION_ERROR_DISK_FULL:
        case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
        case DVC_INGESTION_ERROR_OTHER:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.ERROR);
          break;
        case START_OF_BUFFER_REPLAY_RECEIVED:
        case DROPPED:
        case WARNING:
        case ARCHIVED:
        case UNKNOWN:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.UNKNOWN);
          break;
        default:
          /** Newly added ExecutionStatus should be mapped properly in
           * {@link VenicePushJob.getPerColoPushJobDetailsStatusFromExecutionStatus}
           * and this test should be updated accordingly
           */
          fail(status + " is not mapped properly in getPerColoPushJobDetailsStatusFromExecutionStatus");
      }
    }
  }

  /**
   * Tests that the error message for the {@link com.linkedin.venice.PushJobCheckpoints#RECORD_TOO_LARGE_FAILED} code path of
   * {@link VenicePushJob#updatePushJobDetailsWithJobDetails(DataWriterTaskTracker)} uses maxRecordSizeBytes.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testUpdatePushJobDetailsWithJobDetailsRecordTooLarge(boolean chunkingEnabled) {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      // Setup push job settings and mocks
      PushJobDetails pushJobDetails = vpj.getPushJobDetails();
      pushJobDetails.chunkingEnabled = chunkingEnabled;
      setPushJobSettingDefaults(vpj.getPushJobSetting());
      vpj.setInputStorageQuotaTracker(mock(InputStorageQuotaTracker.class));
      final DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      doReturn(1L).when(dataWriterTaskTracker).getRecordTooLargeFailureCount();

      // The value of chunkingEnabled should dictate the error message returned
      final String errorMessage = vpj.updatePushJobDetailsWithJobDetails(dataWriterTaskTracker);
      final int latestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
      Assert.assertTrue(errorMessage.contains((chunkingEnabled) ? "100.0 MiB" : "950.0 KiB"), errorMessage);
      Assert.assertEquals(latestCheckpoint, PushJobCheckpoints.RECORD_TOO_LARGE_FAILED.getValue());
    }
  }

  /**
   * Tests that the error message for the {@link com.linkedin.venice.PushJobCheckpoints#RECORD_TOO_LARGE_FAILED} code path of
   * {@link VenicePushJob#updatePushJobDetailsWithJobDetails(DataWriterTaskTracker)} uses maxRecordSizeBytes.
   */
  @Test(dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testUpdatePushJobDetailsWithJobDetailsRecordTooLargeWithCompression(
      boolean enableUncompressedMaxRecordSizeLimit,
      CompressionStrategy compressionStrategy) {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      // Setup push job settings and mocks
      PushJobDetails pushJobDetails = vpj.getPushJobDetails();

      setPushJobSettingDefaults(vpj.getPushJobSetting());
      vpj.setInputStorageQuotaTracker(mock(InputStorageQuotaTracker.class));
      vpj.getPushJobSetting().enableUncompressedRecordSizeLimit = enableUncompressedMaxRecordSizeLimit;
      vpj.getPushJobSetting().storeCompressionStrategy = compressionStrategy;

      final DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      doReturn(1L).when(dataWriterTaskTracker).getRecordTooLargeFailureCount();
      doReturn(1L).when(dataWriterTaskTracker).getUncompressedRecordTooLargeFailureCount();

      // The value of chunkingEnabled should dictate the error message returned
      final String errorMessage = vpj.updatePushJobDetailsWithJobDetails(dataWriterTaskTracker);
      Assert.assertTrue(
          errorMessage.contains("records that exceed the maximum record limit of"),
          "Unexpected error message: " + errorMessage);

      if (compressionStrategy.isCompressionEnabled()) {
        if (enableUncompressedMaxRecordSizeLimit) {
          Assert.assertTrue(errorMessage.contains("before compression"), "Unexpected error message: " + errorMessage);
        } else {
          Assert.assertTrue(errorMessage.contains("after compression"), "Unexpected error message: " + errorMessage);
        }
      }

      final int latestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
      Assert.assertEquals(latestCheckpoint, PushJobCheckpoints.RECORD_TOO_LARGE_FAILED.getValue());
    }
  }

  /**
   * These are mainly for code coverage for the code paths of {@link VenicePushJob#getVeniceWriter(PushJobSetting)} and
   * {@link VenicePushJob#getVeniceWriterProperties(PushJobSetting)}.
   */
  @Test
  public void testGetVeniceWriter() {
    Properties props = getVpjRequiredProperties();
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 1000);
    try (final VenicePushJob vpj = getSpyVenicePushJob(props, getClient())) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      setPushJobSettingDefaults(pushJobSetting);
      final VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = vpj.getVeniceWriter(pushJobSetting);
      Assert.assertNotNull(veniceWriter, "VeniceWriter should've been constructed and returned");
      Assert.assertEquals(veniceWriter, vpj.getVeniceWriter(pushJobSetting), "Second get() should return same object");
    }
  }

  @Test
  public void testTargetRegionPushWithDeferredSwapSettings() {
    Properties props = getVpjRequiredProperties();
    String regions = "test1, test2";
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_LIST, regions);

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, getClient())) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertEquals(pushJobSetting.deferVersionSwap, true);
      Assert.assertEquals(pushJobSetting.isTargetRegionPushWithDeferredSwapEnabled, true);
      Assert.assertEquals(pushJobSetting.targetedRegions, regions);
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*cannot be enabled at the same time.*")
  public void testEnableBothTargetRegionConfigs() {
    Properties props = getVpjRequiredProperties();
    String regions = "test1, test2";
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(TARGETED_REGION_PUSH_LIST, regions);

    getSpyVenicePushJob(props, getClient());
  }

  @Test
  public void testConfigureWithMaterializedViewConfigs() throws Exception {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    JobStatusQueryResponse response = mockJobStatusQuery();
    ControllerClient client = getClient();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNull(pushJobSetting.materializedViewConfigFlatMap);
    }
    Map<String, ViewConfig> viewConfigs = new HashMap<>();
    MaterializedViewParameters.Builder builder =
        new MaterializedViewParameters.Builder("testView").setPartitionCount(12)
            .setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    viewConfigs.put("testView", new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build()));
    viewConfigs
        .put("dummyView", new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), Collections.emptyMap()));
    Version version = new VersionImpl(TEST_STORE, 1, TEST_PUSH);
    version.setViewConfigs(viewConfigs);
    client = getClient(storeInfo -> {
      storeInfo.setViewConfigs(viewConfigs);
      storeInfo.setVersions(Collections.singletonList(version));
    }, true);
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    MultiSchemaResponse valueSchemaResponse = getMultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[1];
    schemas[0] = getBasicSchema();
    valueSchemaResponse.setSchemas(schemas);
    doReturn(valueSchemaResponse).when(client).getAllValueSchema(TEST_STORE);
    doReturn(getMultiSchemaResponse()).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    doReturn(getKeySchemaResponse()).when(client).getKeySchema(TEST_STORE);
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNotNull(pushJobSetting.materializedViewConfigFlatMap);
      Map<String, ViewConfig> viewConfigMap =
          ViewUtils.parseViewConfigMapString(pushJobSetting.materializedViewConfigFlatMap);
      // Ensure only materialized view configs are propagated to the job settings
      Assert.assertEquals(viewConfigMap.size(), 1);
      Assert.assertTrue(viewConfigMap.containsKey("testView"));
      Assert.assertEquals(viewConfigMap.get("testView").getViewClassName(), MaterializedView.class.getCanonicalName());
    }
  }

  @Test
  public void testConfigureWithMaterializedViewConfigsWithFlinkVeniceViewsEnabled() throws Exception {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    JobStatusQueryResponse response = mockJobStatusQuery();
    ControllerClient client = getClient();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNull(pushJobSetting.materializedViewConfigFlatMap);
    }
    Map<String, ViewConfig> viewConfigs = new HashMap<>();
    MaterializedViewParameters.Builder builder =
        new MaterializedViewParameters.Builder("testView").setPartitionCount(12)
            .setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    viewConfigs.put("testView", new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build()));
    viewConfigs
        .put("dummyView", new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), Collections.emptyMap()));
    Version version = new VersionImpl(TEST_STORE, 1, TEST_PUSH);
    version.setViewConfigs(viewConfigs);
    client = getClient(storeInfo -> {
      storeInfo.setViewConfigs(viewConfigs);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setFlinkVeniceViewsEnabled(true); // Enable Flink Venice Views at store level
    }, true);
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    MultiSchemaResponse valueSchemaResponse = getMultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[1];
    schemas[0] = getBasicSchema();
    valueSchemaResponse.setSchemas(schemas);
    doReturn(valueSchemaResponse).when(client).getAllValueSchema(TEST_STORE);
    doReturn(getMultiSchemaResponse()).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    doReturn(getKeySchemaResponse()).when(client).getKeySchema(TEST_STORE);
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNull(pushJobSetting.materializedViewConfigFlatMap); // Should be null since Flink Venice Views is
                                                                       // enabled
    }
  }

  @Test
  public void testTargetedRegionPushWithDeferredSwapConfigValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, false);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Targeted region push list is only supported when targeted region push is enabled");
    }

    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(INCREMENTAL_PUSH, true);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Incremental push is not supported while using targeted region push mode");
    }

    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(DEFER_VERSION_SWAP, true);
    props.put(INCREMENTAL_PUSH, false);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(
          e.getMessage(),
          "Target region push with deferred swap and deferred swap cannot be enabled at the same time");
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Target region list cannot contain all regions.*")
  public void testTargetRegionPushWithAllRegions() {
    Properties props = getVpjRequiredProperties();
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0, dc-1");

    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          put("dc-0", 1);
          put("dc-1", 1);
        }
      });
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      pushJob.run();
    }
  }

  /**
   * Test that START_VERSION_SWAP checkpoint is invoked when target region push with deferred swap is enabled.
   */
  @Test
  public void testVersionSwapCheckpoint() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");

    // Create a version with ONLINE status to simulate successful version swap
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(VersionStatus.ONLINE);

    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          put("dc-0", 1);
          put("dc-1", 1);
        }
      });
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setLargestUsedVersionNumber(1);
      storeInfo.setTargetRegionSwapWaitTime(60); // 60 minutes wait time
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      // Mock job status query to return COMPLETED
      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      pushJob.run();

      // Verify that START_VERSION_SWAP checkpoint is called
      verify(pushJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_VERSION_SWAP);

      // Verify that COMPLETE_VERSION_SWAP checkpoint is called
      verify(pushJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.COMPLETE_VERSION_SWAP);

      // Get the actual PushJobDetails object
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();

      // Verify that COMPLETED status was added to overallStatus
      boolean foundCompletedStatus = false;
      for (PushJobDetailsStatusTuple statusTuple: pushJobDetails.overallStatus) {
        if (statusTuple.status == PushJobDetailsStatus.COMPLETED.getValue()) {
          foundCompletedStatus = true;
          break;
        }
      }
      assertTrue(foundCompletedStatus);
    }
  }

  @Test
  public void testValidateRegularPushWithTTLRepush() {
    ControllerClient mockControllerClient = mock(ControllerClient.class);
    StoreResponse mockStoreResponse = mock(StoreResponse.class);
    doReturn(mockStoreResponse).when(mockControllerClient).getStore(anyString());
    doReturn(false).when(mockStoreResponse).isError();
    StoreInfo mockStoreInfo = mock(StoreInfo.class);
    doReturn(mockStoreInfo).when(mockStoreResponse).getStore();
    doReturn(true).when(mockStoreInfo).isTTLRepushEnabled();
    // Re-push, incremental and empty pushes should be allowed
    Properties props = getVpjRequiredProperties();
    VenicePushJob venicePushJob = new VenicePushJob(PUSH_JOB_ID, props);
    PushJobSetting pushJobSetting = venicePushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = true;
    pushJobSetting.isIncrementalPush = true;
    venicePushJob.checkRegularPushWithTTLRepush(mockControllerClient, venicePushJob.getPushJobSetting());
    venicePushJob = new VenicePushJob(PUSH_JOB_ID, props);
    pushJobSetting = venicePushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = true;
    pushJobSetting.isSourceKafka = true;
    venicePushJob.checkRegularPushWithTTLRepush(mockControllerClient, venicePushJob.getPushJobSetting());
    venicePushJob = new VenicePushJob(PUSH_JOB_ID, props);
    pushJobSetting = venicePushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = false;
    venicePushJob.checkRegularPushWithTTLRepush(mockControllerClient, venicePushJob.getPushJobSetting());
    // Regular batch push should be rejected
    final VenicePushJob failPushJob = new VenicePushJob(PUSH_JOB_ID, props);
    pushJobSetting = failPushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = true;
    Assert.assertThrows(
        VeniceException.class,
        () -> failPushJob.checkRegularPushWithTTLRepush(mockControllerClient, failPushJob.getPushJobSetting()));
  }

  private SchemaResponse getKeySchemaResponse() {
    SchemaResponse response = new SchemaResponse();
    response.setId(1);
    response.setCluster(TEST_CLUSTER);
    response.setName(TEST_STORE);
    response.setSchemaStr(KEY_SCHEMA_STR);
    return response;
  }

  private MultiSchemaResponse.Schema getBasicSchema() {
    MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
    schema.setSchemaStr(VALUE_SCHEMA_STR);
    schema.setId(1);
    schema.setRmdValueSchemaId(1);
    return schema;
  }

  private MultiSchemaResponse getMultiSchemaResponse() {
    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    multiSchemaResponse.setCluster(TEST_CLUSTER);
    multiSchemaResponse.setName(TEST_STORE);
    multiSchemaResponse.setSuperSetSchemaId(1);
    multiSchemaResponse.setSchemas(new MultiSchemaResponse.Schema[0]);
    return multiSchemaResponse;
  }

  private JobStatusQueryResponse mockJobStatusQuery() {
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setStatus(ExecutionStatus.COMPLETED.toString());
    response.setStatusDetails("nothing");
    response.setVersion(1);
    response.setName(TEST_STORE);
    response.setCluster(TEST_CLUSTER);
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
    extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
    response.setExtraInfo(extraInfo);
    return response;
  }

  private VersionCreationResponse mockVersionCreationResponse(ControllerClient client) {
    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaTopic("kafka-topic");
    versionCreationResponse.setVersion(1);
    versionCreationResponse.setKafkaSourceRegion("dc-0");
    versionCreationResponse.setKafkaBootstrapServers("kafka-bootstrap-server");
    versionCreationResponse.setPartitions(1);
    versionCreationResponse.setEnableSSL(false);
    versionCreationResponse.setCompressionStrategy(CompressionStrategy.NO_OP);
    versionCreationResponse.setDaVinciPushStatusStoreEnabled(false);
    versionCreationResponse.setPartitionerClass(DefaultVenicePartitioner.class.getCanonicalName());
    versionCreationResponse.setPartitionerParams(Collections.emptyMap());

    if (client != null) {
      doReturn(versionCreationResponse).when(client)
          .requestTopicForWrites(
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
              anyBoolean(),
              anyInt());
    }

    return versionCreationResponse;
  }

  private void skipVPJValidation(VenicePushJob pushJob) throws Exception {
    doAnswer(invocation -> {
      VeniceProperties properties = pushJob.getJobProperties();
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();

      if (!pushJobSetting.isSourceKafka) {
        Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(SIMPLE_FILE_SCHEMA_STR);
        pushJobSetting.keyField = properties.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
        pushJobSetting.valueField = properties.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);

        pushJobSetting.inputDataSchema = schema;
        pushJobSetting.valueSchema = schema.getField(pushJobSetting.valueField).schema();

        pushJobSetting.inputDataSchemaString = SIMPLE_FILE_SCHEMA_STR;
        pushJobSetting.keySchema = pushJobSetting.inputDataSchema.getField(pushJobSetting.keyField).schema();

        pushJobSetting.keySchemaString = pushJobSetting.keySchema.toString();
        pushJobSetting.valueSchemaString = pushJobSetting.valueSchema.toString();
      }

      return getMockInputDataInfoProvider();
    }).when(pushJob).getInputDataInfoProvider();

    doNothing().when(pushJob).validateKeySchema(any());
    doNothing().when(pushJob).validateAndRetrieveValueSchemas(any(), any(), anyBoolean());
    doNothing().when(pushJob).runJobAndUpdateStatus();
  }

  /** Sets basic values for the given {@link PushJobSetting} object in order for VeniceWriter to be constructed. */
  private void setPushJobSettingDefaults(PushJobSetting setting) {
    setting.storeName = TEST_STORE;
    setting.kafkaUrl = "localhost:9092";
    setting.partitionerParams = new HashMap<>();
    setting.partitionerClass = DefaultVenicePartitioner.class.getCanonicalName();
    setting.topic = Version.composeKafkaTopic(setting.storeName, 7);
    setting.partitionCount = 1;
    setting.maxRecordSizeBytes = 100 * BYTES_PER_MB;
  }

  @DataProvider(name = "versionStatuses")
  public Object[][] versionStatuses() {
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
    extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());

    Map<String, String> extraInfo2 = new HashMap<>();
    extraInfo2.put("dc-0", ExecutionStatus.COMPLETED.toString());
    extraInfo2.put("dc-1", ExecutionStatus.COMPLETED.toString());

    return new Object[][] { { VersionStatus.ONLINE, extraInfo }, { VersionStatus.ERROR, extraInfo2 },
        { VersionStatus.PARTIALLY_ONLINE, extraInfo }, { VersionStatus.KILLED, extraInfo } };
  }

  @Test(dataProvider = "versionStatuses")
  public void testTargetRegionPushWithDeferredSwapVersionStatusChecks(
      VersionStatus versionStatus,
      Map<String, String> extraInfo) {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    properties.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(versionStatus);
    ControllerClient client = getClient(store -> {
      store.setVersions(Collections.singletonList(version));
      store.setLargestUsedVersionNumber(1);
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      response.setExtraInfo(extraInfo);
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      pushJob.run();
    } catch (Exception e) {
      if (VersionStatus.PARTIALLY_ONLINE.equals(versionStatus)) {
        Assert.assertEquals(
            e.getMessage(),
            "Version kafka-topic is only partially online in some regions. Check nuage to see which regions are not serving the latest version."
                + " It is possible that there was a failure in rolling forward on the controller side or ingestion failed in some regions.");
      } else if (VersionStatus.KILLED.equals(versionStatus)) {
        Assert.assertEquals(e.getMessage(), "Version kafka-topic was killed and cannot be served.");
      } else {
        Assert.assertEquals(
            e.getMessage(),
            "Version kafka-topic was rolled back after ingestion completed due to validation failure");
      }
    }
  }
}
