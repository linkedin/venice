package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.LEGACY_AVRO_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.LEGACY_AVRO_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.MULTI_REGION;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.POST_VALIDATION_CONSUMPTION_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SOURCE_ETL;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_UPDATE_SCHEMA;
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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceValidationException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.testng.Assert;
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

  private static final String SIMPLE_FILE_SCHEMA_STR = "{\n" + "    \"namespace\": \"example.avro\",\n"
      + "    \"type\": \"record\",\n" + "    \"name\": \"User\",\n" + "    \"fields\": [\n"
      + "      { \"name\": \"id\", \"type\": \"string\" },\n" + "      { \"name\": \"name\", \"type\": \"string\" },\n"
      + "      { \"name\": \"age\", \"type\": \"int\" },\n" + "      { \"name\": \"company\", \"type\": \"string\" }\n"
      + "    ]\n" + "  }";

  @Test
  public void testVPJcheckInputUpdateSchema() {
    VenicePushJob vpj = mock(VenicePushJob.class);
    when(vpj.isUpdateSchema(anyString())).thenCallRealMethod();
    Assert.assertTrue(vpj.isUpdateSchema(NAME_RECORD_V1_UPDATE_SCHEMA.toString()));
    Assert.assertFalse(vpj.isUpdateSchema(NAME_RECORD_V1_SCHEMA.toString()));
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
    Properties repushProps = getRepushReadyProps();

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
  public void testPushJobSettingWithD2Routing() {
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null, null));
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
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null, null));
    });
    try (VenicePushJob pushJob = getSpyVenicePushJob(vpjProps, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.livenessHeartbeatEnabled);
    }
  }

  @Test
  public void testPushJobPollStatus() {
    Properties vpjProps = new Properties();
    vpjProps.setProperty(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "true");
    ControllerClient client = mock(ControllerClient.class);
    JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
    doReturn("UNKNOWN").when(response).getStatus();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), eq(Optional.empty()), eq(null));
    try (VenicePushJob pushJob = getSpyVenicePushJob(vpjProps, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      pushJobSetting.jobStatusInUnknownStateTimeoutMs = 10;
      Assert.assertTrue(pushJobSetting.livenessHeartbeatEnabled);
      pushJobSetting.version = 1;
      pushJobSetting.topic = "abc";
      pushJobSetting.storeResponse = new StoreResponse();
      pushJobSetting.storeResponse.setName("abc");
      StoreInfo storeInfo = new StoreInfo();
      storeInfo.setBootstrapToOnlineTimeoutInHours(0);
      pushJobSetting.storeResponse.setStore(storeInfo);
      VeniceException exception = Assert.expectThrows(
          VeniceException.class,
          () -> pushJob.pollStatusUntilComplete(null, client, pushJobSetting, null, false));
      Assert
          .assertEquals(exception.getMessage(), "Failing push-job for store abc which is still running after 0 hours.");
    }
  }

  private Properties getRepushReadyProps() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps.setProperty(SOURCE_KAFKA, "true");
    repushProps.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, REPUSH_VERSION));
    repushProps.setProperty(KAFKA_INPUT_BROKER_URL, "localhost");
    repushProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    return repushProps;
  }

  private VenicePushJob getSpyVenicePushJob(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJProps(TEST_URL, TEST_PATH, TEST_STORE);
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
        TEST_STORE);
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
        TEST_STORE);
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
    Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
    version.setHybridStoreConfig(storeInfo.getHybridStoreConfig());
    storeInfo.setVersions(Collections.singletonList(version));
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
        Assert.fail("Should throw UndefinedPropertyException for missing property: " + prop);
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

  @Test(dataProvider = "Four-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testShouldBuildZstdCompressionDictionary(
      boolean compressionMetricCollectionEnabled,
      boolean isSourceKafka,
      boolean isIncrementalPush,
      boolean inputFileHasRecords) {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.compressionMetricCollectionEnabled = compressionMetricCollectionEnabled;
    pushJobSetting.isSourceKafka = isSourceKafka;
    pushJobSetting.isIncrementalPush = isIncrementalPush;

    for (CompressionStrategy compressionStrategy: CompressionStrategy.values()) {
      pushJobSetting.storeCompressionStrategy = compressionStrategy;

      if (isSourceKafka || isIncrementalPush) {
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

    // Test with isSourceKafka == true
    pushJobSetting.isSourceKafka = true;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // reset settings for the below tests
    pushJobSetting.isSourceKafka = false;

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
      Assert.fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Targeted region push list is only supported when targeted region push is enabled");
    }

    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(INCREMENTAL_PUSH, true);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      Assert.fail("Test should fail, but doesn't.");
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
    props.put(POST_VALIDATION_CONSUMPTION_ENABLED, false);
    // when targeted region push is enabled, but store doesn't have source fabric set.
    ControllerClient client = getClient(store -> {
      store.setNativeReplicationSourceFabric("");
    });
    JobStatusQueryResponse response = mockJobStatusQuery();

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString());
      skipVPJValidation(pushJob);
      try {
        pushJob.run();
        Assert.fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "The store either does not have native replication mode enabled or set up default source fabric."));
      }
    }

    props.put(TARGETED_REGION_PUSH_LIST, "dc-0, dc-1");
    client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString());
      skipVPJValidation(pushJob);
      pushJob.run();
      Assert.assertEquals(pushJob.getPushJobSetting().targetedRegions, "dc-0, dc-1");
    }
  }

  @Test
  public void testTargetedRegionPushReporting() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(POST_VALIDATION_CONSUMPTION_ENABLED, false);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0, dc-1");
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = response.getExtraInfo();
      // one of the regions failed, so should fail
      extraInfo.put("dc-0", ExecutionStatus.NOT_STARTED.toString());
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString());
      try {
        pushJob.run();
        Assert.fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(e.getMessage().contains("Push job error"));
      }

      extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
      extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
      // both regions completed, so should succeed
      pushJob.run();
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationConsumptionForBatchStore() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(POST_VALIDATION_CONSUMPTION_ENABLED, true);
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString());

      VersionCreationResponse mockVersionCreationResponse = mockVersionCreationResponse(client);
      mockVersionCreationResponse.setKafkaSourceRegion(null);

      // verify the kafka source region must be present when kick off post-validation consumption
      try {
        pushJob.run();
        Assert.fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(
            e.getMessage().contains("Post-validation consumption halted due to no available source region found"));
      }
      mockVersionCreationResponse.setKafkaSourceRegion("dc-0");
      verify(pushJob, times(1)).postPushValidation();

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

      ControllerResponse goodDataRecoveryResponse = new ControllerResponse();
      doReturn(goodDataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());

      // the job should succeed
      pushJob.run();
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationConsumptionForHybridStore() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(POST_VALIDATION_CONSUMPTION_ENABLED, true);
    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null, null));
    }, true);
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString());
      // skip the mocking for repush
      doCallRealMethod().doNothing().when(pushJob).run();
      pushJob.run();

      // for hybrid store, the job is supposed to ran twice, one for targeted region push and another is for repush
      verify(pushJob, times(2)).run();
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationFailedForValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(POST_VALIDATION_CONSUMPTION_ENABLED, true);
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString());
      mockVersionCreationResponse(client);

      doThrow(new VeniceValidationException("error")).when(pushJob).postPushValidation();

      assertThrows(VeniceValidationException.class, pushJob::run);
      verify(pushJob, never()).postValidationConsumption(any());
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "D2 service name and zk host must be provided when system schema reader is enabled")
  public void testEnableSchemaReaderConfigValidation() {
    Properties props = getVpjRequiredProperties();
    props.put(SYSTEM_SCHEMA_READER_ENABLED, true);
    VenicePushJob pushJob = spy(new VenicePushJob(PUSH_JOB_ID, props));
    doReturn("test_store_v1").when(pushJob).getSourceTopicNameForKafkaInput(anyString(), any());
    RepushInfoResponse repushInfoResponse = new RepushInfoResponse();
    RepushInfo repushInfo = new RepushInfo();
    repushInfo.setSystemSchemaClusterD2ServiceName("cluster0");
    repushInfoResponse.setRepushInfo(repushInfo);
    pushJob.getPushJobSetting().repushInfoResponse = repushInfoResponse;
    pushJob.initKIFRepushDetails();
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
              any());
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

        pushJobSetting.fileSchema = schema;
        pushJobSetting.valueSchema = schema.getField(pushJobSetting.valueField).schema();

        pushJobSetting.fileSchemaString = SIMPLE_FILE_SCHEMA_STR;
        pushJobSetting.keySchema = pushJobSetting.fileSchema.getField(pushJobSetting.keyField).schema();

        pushJobSetting.keySchemaString = pushJobSetting.keySchema.toString();
        pushJobSetting.valueSchemaString = pushJobSetting.valueSchema.toString();
      }

      return getMockInputDataInfoProvider();
    }).when(pushJob).getInputDataInfoProvider();

    doNothing().when(pushJob).validateKeySchema(any());
    doNothing().when(pushJob).validateValueSchema(any(), any(), anyBoolean());
    doNothing().when(pushJob).runJobAndUpdateStatus();
  }
}
