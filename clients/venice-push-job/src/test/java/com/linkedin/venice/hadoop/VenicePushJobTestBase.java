package com.linkedin.venice.hadoop;

import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.vpj.VenicePushJobConstants.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.mapreduce.datawriter.jobs.DataWriterMRJob;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.testng.annotations.DataProvider;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */

abstract class VenicePushJobTestBase {
  protected static final String TEST_PUSH = "test_push";
  protected static final String TEST_URL = "test_url";
  protected static final String TEST_PATH = "test_path";
  protected static final String TEST_STORE = "test_store";
  protected static final String TEST_CLUSTER = "test_cluster";
  protected static final String TEST_SERVICE = "test_venice";
  protected static final int REPUSH_VERSION = 1;

  protected static final String TEST_PARENT_ZK_ADDRESS = "localhost:2180";
  protected static final String TEST_ZK_ADDRESS = "localhost:2181";
  protected static final String TEST_PARENT_CONTROLLER_D2_SERVICE = "ParentController";
  protected static final String TEST_CHILD_CONTROLLER_D2_SERVICE = "ChildController";

  protected static final String PUSH_JOB_ID = "push_job_number_101";
  protected static final String DISCOVERY_URL = "d2://d2Clusters/venice-discovery";
  protected static final String PARENT_REGION_NAME = "dc-parent";

  protected static final String KEY_SCHEMA_STR = "\"string\"";
  protected static final String VALUE_SCHEMA_STR = "\"string\"";

  protected static final String RMD_SCHEMA_STR = "{\"type\":\"record\",\"name\":\"__replication_metadata\","
      + "\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"_venice_replication_checkpoint_vector\","
      + "\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[]},{\"name\":\"_venice_timestamp\","
      + "\"type\":\"long\",\"default\":0}]}";

  protected static final String RMD_SCHEMA_STR_V2 = "{\"type\":\"record\",\"name\":\"__replication_metadata\","
      + "\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"_venice_replication_checkpoint_vector\","
      + "\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[]},{\"name\":\"_venice_timestamp\","
      + "\"type\":\"long\",\"default\":0},{\"name\":\"_venice_extra_field\",\"type\":\"string\",\"default\":\"\"}]}";

  protected static final String SIMPLE_FILE_SCHEMA_STR = "{\n" + "    \"namespace\": \"example.avro\",\n"
      + "    \"type\": \"record\",\n" + "    \"name\": \"User\",\n" + "    \"fields\": [\n"
      + "      { \"name\": \"id\", \"type\": \"string\" },\n" + "      { \"name\": \"name\", \"type\": \"string\" },\n"
      + "      { \"name\": \"age\", \"type\": \"int\" },\n" + "      { \"name\": \"company\", \"type\": \"string\" }\n"
      + "    ]\n" + "  }";

  protected Properties getRepushWithTTLProps() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps.setProperty(SOURCE_KAFKA, "true");
    repushProps.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, REPUSH_VERSION));
    repushProps.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost");
    repushProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    return repushProps;
  }

  protected VenicePushJob getSpyVenicePushJob(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJProps(TEST_URL, TEST_PATH, TEST_STORE, Collections.emptyMap());
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  protected VenicePushJob getSpyVenicePushJobWithD2Routing(Properties props, ControllerClient client) {
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

  protected VenicePushJob getSpyVenicePushJobWithMultiRegionD2Routing(Properties props, ControllerClient client) {
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

  protected VenicePushJob getSpyVenicePushJobInternal(
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

  protected ControllerClient getClient() {
    return getClient(storeInfo -> {}, false);
  }

  protected ControllerClient getClient(Consumer<StoreInfo> storeInfo) {
    return getClient(storeInfo, false);
  }

  protected ControllerClient getClient(Consumer<StoreInfo> storeInfo, boolean applyFirst) {
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
    doReturn(response).when(client).writeEndOfPush(anyString(), anyInt(), any());
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
  protected StoreInfo getStoreInfo(Consumer<StoreInfo> info, boolean applyFirst) {
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

  protected InputDataInfoProvider getMockInputDataInfoProvider() throws Exception {
    InputDataInfoProvider provider = mock(InputDataInfoProvider.class);
    // Input file size cannot be zero.
    InputDataInfoProvider.InputDataInfo info =
        new InputDataInfoProvider.InputDataInfo(10L, 1, false, System.currentTimeMillis());
    doReturn(info).when(provider).validateInputAndGetInfo(anyString());
    return provider;
  }

  protected Properties getVpjRequiredProperties() {
    Properties props = new Properties();
    props.put(INPUT_PATH_PROP, TEST_PATH);
    props.put(VENICE_DISCOVER_URL_PROP, DISCOVERY_URL);
    props.put(MULTI_REGION, true);
    props.put(PARENT_CONTROLLER_REGION_NAME, PARENT_REGION_NAME);
    props.put(D2_ZK_HOSTS_PREFIX + PARENT_REGION_NAME, TEST_PARENT_ZK_ADDRESS);
    props.put(VENICE_STORE_NAME_PROP, TEST_STORE);
    return props;
  }

  protected SchemaResponse getKeySchemaResponse() {
    SchemaResponse response = new SchemaResponse();
    response.setId(1);
    response.setCluster(TEST_CLUSTER);
    response.setName(TEST_STORE);
    response.setSchemaStr(KEY_SCHEMA_STR);
    return response;
  }

  protected MultiSchemaResponse.Schema getBasicSchema() {
    MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
    schema.setSchemaStr(VALUE_SCHEMA_STR);
    schema.setId(1);
    schema.setRmdValueSchemaId(1);
    return schema;
  }

  protected MultiSchemaResponse getMultiSchemaResponse() {
    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    multiSchemaResponse.setCluster(TEST_CLUSTER);
    multiSchemaResponse.setName(TEST_STORE);
    multiSchemaResponse.setSuperSetSchemaId(1);
    multiSchemaResponse.setSchemas(new MultiSchemaResponse.Schema[0]);
    return multiSchemaResponse;
  }

  protected JobStatusQueryResponse mockJobStatusQuery() {
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

  protected VersionCreationResponse mockVersionCreationResponse(ControllerClient client) {
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

  protected void skipVPJValidation(VenicePushJob pushJob) throws Exception {
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
  protected void setPushJobSettingDefaults(PushJobSetting setting) {
    setting.storeName = TEST_STORE;
    setting.pushDestinationPubsubBroker = "localhost:9092";
    setting.partitionerParams = new HashMap<>();
    setting.partitionerClass = DefaultVenicePartitioner.class.getCanonicalName();
    setting.topic = Version.composeKafkaTopic(setting.storeName, 7);
    setting.partitionCount = 1;
    setting.maxRecordSizeBytes = 100 * BYTES_PER_MB;
  }

  @DataProvider(name = "DataWriterJobClasses")
  public Object[][] getDataWriterJobClasses() {
    return new Object[][] { { DataWriterMRJob.class }, { DataWriterSparkJob.class } };
  }

  @DataProvider(name = "versionStatuses")
  public Object[][] versionStatuses() {
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
    extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());

    Map<String, String> extraInfo2 = new HashMap<>();
    extraInfo2.put("dc-0", ExecutionStatus.COMPLETED.toString());
    extraInfo2.put("dc-1", ExecutionStatus.COMPLETED.toString());

    return new Object[][] { { VersionStatus.ONLINE, extraInfo }, { VersionStatus.ROLLED_BACK, extraInfo2 },
        { VersionStatus.PARTIALLY_ONLINE, extraInfo }, { VersionStatus.KILLED, extraInfo } };
  }
}
