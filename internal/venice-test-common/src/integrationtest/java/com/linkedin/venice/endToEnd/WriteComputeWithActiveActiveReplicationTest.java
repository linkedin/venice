package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestPushUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.config.MapConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class WriteComputeWithActiveActiveReplicationTest {
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final int PUSH_TIMEOUT = TEST_TIMEOUT / 2;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  public static final String REGULAR_FIELD = "RegularField";
  public static final String INT_ARRAY_FIELD = "IntArrayField";
  public static final String MAP_FIELD = "MapField";

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;
  private ControllerClient parentControllerClient;
  private ControllerClient dc0Client;
  private ControllerClient dc1Client;
  private List<ControllerClient> dcControllerClientList;
  private String dc0RouterUrl;
  private String dc1RouterUrl;

  private final String KEY_SCHEMA_STR = "{\"type\" : \"string\"}";
  private final String PERSON_F1_NAME = "name";
  private final String PERSON_F2_NAME = "age";
  private final String PERSON_F3_NAME = "hometown";

  private String storeName;
  private Map<VeniceMultiClusterWrapper, VeniceSystemProducer> systemProducerMap;

  private Map<String, AvroGenericStoreClient<String, GenericRecord>> storeClients;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws IOException {
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);

    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.of(new VeniceProperties(serverProperties)),
        false);

    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
    childDatacenters = multiColoMultiClusterWrapper.getClusters();

    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
    dc0RouterUrl = childDatacenters.get(0).getClusters().get(clusterName).getRandomRouterURL();
    dc1RouterUrl = childDatacenters.get(1).getClusters().get(clusterName).getRandomRouterURL();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(dc0Client);
    Utils.closeQuietlyWithErrorLogged(dc1Client);
    Utils.closeQuietlyWithErrorLogged(multiColoMultiClusterWrapper);
  }

  @BeforeMethod
  private void setupStore() {
    storeName = Utils.getUniqueString("test-store-aa-wc");
    storeClients = new HashMap<>(2);
  }

  @AfterMethod
  public void cleanUpAfterMethod() {
    try {
      parentControllerClient.disableAndDeleteStore(storeName);
    } catch (Exception e) {
      // ignore... this is just best-effort.
    }
  }

  private void runEmptyPushAndVerifyStoreVersion(String storeName, int expectedStoreVersion) {
    // Empty push to create a version
    assertCommand(
        parentControllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, PUSH_TIMEOUT),
        "Empty push did not complete in " + PUSH_TIMEOUT + "seconds");

    // Verify that version is created in all regions
    for (ControllerClient dcClient: dcControllerClientList) {
      waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = assertCommand(dcClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), expectedStoreVersion);
      });
    }
  }

  // create one system store per region
  private void startVeniceSystemProducers() {
    systemProducerMap = new HashMap<>(NUMBER_OF_CHILD_DATACENTERS);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    for (int dcId = 0; dcId < NUMBER_OF_CHILD_DATACENTERS; dcId++) {
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(dcId);
      Map<String, String> samzaConfig = new HashMap<>();
      samzaConfig.put(SYSTEMS_PREFIX + "venice." + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
      samzaConfig.put(SYSTEMS_PREFIX + "venice." + VENICE_STORE, storeName);
      samzaConfig.put(SYSTEMS_PREFIX + "venice." + VENICE_AGGREGATE, "false");
      samzaConfig.put(DEPLOYMENT_ID, "dcId_" + dcId + "_" + storeName);
      samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, childDataCenter.getZkServerWrapper().getAddress());
      samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, multiColoMultiClusterWrapper.getZkServerWrapper().getAddress());
      samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
      samzaConfig.put(SSL_ENABLED, "false");
      VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();
      systemProducerMap.put(childDataCenter, veniceProducer);
    }
  }

  /*
   * Verify write-compute/partial-update on field level in Active-Active replication setup
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAAReplicationForPartialUpdateOnFields() throws IOException {
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV1.avsc"));
    Schema valueSchemaV2 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV2.avsc"));
    Schema wcSchemaV1 = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV1);
    Schema wcSchemaV2 = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV2);

    assertCommand(parentControllerClient.createNewStore(storeName, "owner", KEY_SCHEMA_STR, valueSchemaV1.toString()));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
        .setActiveActiveReplicationEnabled(true)
        .setLeaderFollowerModel(true)
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setChunkingEnabled(false)
        .setIncrementalPushEnabled(true)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setWriteComputationEnabled(true);
    assertCommand(parentControllerClient.updateStore(storeName, params));

    // create a store version and start system producers
    runEmptyPushAndVerifyStoreVersion(storeName, 1);
    startVeniceSystemProducers();

    // PUT
    String key1 = "key1";

    // PartialPut before PUT should succeed with empty values for the fields which were not set using PartialPut
    // todo: Code fix is required to make this work. NPE is thrown in MCR::getValueSchema
    // Update: works with hotfix, i.e., using incomingValueSchemaId when oldSchemaId is -1
    UpdateBuilder ubKv1F0 = new UpdateBuilderImpl(wcSchemaV1);
    ubKv1F0.setNewFieldValue(PERSON_F1_NAME, "Bar");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubKv1F0.build());
    validatePersonV1Record(storeName, dc0RouterUrl, key1, "Bar", -1);
    validatePersonV1Record(storeName, dc1RouterUrl, key1, "Bar", -1);

    // PUT
    GenericRecord val1 = new GenericData.Record(valueSchemaV1);
    val1.put(PERSON_F1_NAME, "Foo");
    val1.put(PERSON_F2_NAME, 42);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    validatePersonV1Record(storeName, dc0RouterUrl, key1, "Foo", 42);
    validatePersonV1Record(storeName, dc1RouterUrl, key1, "Foo", 42);

    // PartialPut
    UpdateBuilder ubKv1F1 = new UpdateBuilderImpl(wcSchemaV1);
    ubKv1F1.setNewFieldValue(PERSON_F1_NAME, "Bar");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubKv1F1.build());
    validatePersonV1Record(storeName, dc0RouterUrl, key1, "Bar", 42);
    validatePersonV1Record(storeName, dc1RouterUrl, key1, "Bar", 42);

    // PartialPut all the fields at the same time
    UpdateBuilder ubKv1All = new UpdateBuilderImpl(wcSchemaV1);
    ubKv1All.setNewFieldValue(PERSON_F1_NAME, "FooBar");
    ubKv1All.setNewFieldValue(PERSON_F2_NAME, 24);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubKv1All.build());
    validatePersonV1Record(storeName, dc0RouterUrl, key1, "FooBar", 24);
    validatePersonV1Record(storeName, dc1RouterUrl, key1, "FooBar", 24);

    // Delete then PartialPut
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, null);
    validatePersonV1Record(storeName, dc0RouterUrl, key1, null, null);
    validatePersonV1Record(storeName, dc1RouterUrl, key1, null, null);
    UpdateBuilder ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setNewFieldValue(PERSON_F1_NAME, "DeleteThenPartialPut");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ub.build());
    validatePersonV1Record(storeName, dc0RouterUrl, key1, "DeleteThenPartialPut", -1);
    validatePersonV1Record(storeName, dc1RouterUrl, key1, "DeleteThenPartialPut", -1);

    /*
     * Case: Updates with lower timestamps should be ignored && updates with higher timestamps should be applied
     * Steps:
     * 1. Put full record containing two fields and timestamp t2. Outcome: full update applied
     * 2. Update the first field with timestamp t1; here t1 < t2 and hence the update should be ignored
     * 3. Update the second field with timestamp t3; here t3 > t2 and hence the update should be applied
     * 4. Update the first field with timestamp t2; since t2 == t2 update should be applied?
     */
    String key2 = "key2";
    GenericRecord val2 = new GenericData.Record(valueSchemaV1);
    val2.put(PERSON_F1_NAME, "val2f1_b");
    val2.put(PERSON_F2_NAME, 20);
    VeniceObjectWithTimestamp val2T2 = new VeniceObjectWithTimestamp(val2, 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, val2T2);
    validatePersonV1Record(storeName, dc0RouterUrl, key2, "val2f1_b", 20);
    validatePersonV1Record(storeName, dc1RouterUrl, key2, "val2f1_b", 20);

    // val2T1 update should be ignored since its timestamp t1 is lower than the existing timestamp t2 for name field
    UpdateBuilder ub2 = new UpdateBuilderImpl(wcSchemaV1);
    ub2.setNewFieldValue(PERSON_F1_NAME, "val2f1_a");
    VeniceObjectWithTimestamp val2T1 = new VeniceObjectWithTimestamp(ub2.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, val2T1);

    // val2T3 update should be applied since its timestamp t3 is greater than the existing timestamp t2 for age field
    UpdateBuilder ub3 = new UpdateBuilderImpl(wcSchemaV1);
    ub3.setNewFieldValue(PERSON_F2_NAME, 40);
    VeniceObjectWithTimestamp val2T3 = new VeniceObjectWithTimestamp(ub3.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, val2T3);
    validatePersonV1Record(storeName, dc0RouterUrl, key2, "val2f1_b", 40);
    validatePersonV1Record(storeName, dc1RouterUrl, key2, "val2f1_b", 40);

    // val2T2Prime update should be applied since its timestamp t2 is equal to the existing timestamp t2 for name field
    // For the field level updates with matching timestamp, winning update is decided using data type comparator
    // (specifically GenericData.compare for non-map types). Since in this case "val2f1_b" < "val2f1_b_prime"
    // "val2f1_b_prime" should win DCR.
    UpdateBuilder ub2Prime = new UpdateBuilderImpl(wcSchemaV1);
    ub2Prime.setNewFieldValue(PERSON_F1_NAME, "val2f1_b_prime");
    VeniceObjectWithTimestamp val2T2Prime = new VeniceObjectWithTimestamp(ub2Prime.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, val2T2Prime);
    validatePersonV1Record(storeName, dc0RouterUrl, key2, "val2f1_b_prime", 40);
    validatePersonV1Record(storeName, dc1RouterUrl, key2, "val2f1_b_prime", 40);

    VeniceObjectWithTimestamp timestampedOp;

    // Delete then PartialPut with the same timestamp
    timestampedOp = new VeniceObjectWithTimestamp(null, 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    validatePersonV1Record(storeName, dc0RouterUrl, key2, null, null);
    validatePersonV1Record(storeName, dc1RouterUrl, key2, null, null);
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setNewFieldValue(PERSON_F1_NAME, "DeleteThenPartialPut");
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    ub.setNewFieldValue(PERSON_F2_NAME, 42);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    validatePersonV1Record(storeName, dc0RouterUrl, key2, "DeleteThenPartialPut", 42);
    validatePersonV1Record(storeName, dc1RouterUrl, key2, "DeleteThenPartialPut", 42);

    /*
    * Case: Add a new field and remove an existing field in the new schema.
    * The final record should contain both removed and added fields.
    */
    String key3 = "key3";
    GenericRecord val3 = new GenericData.Record(valueSchemaV1);
    val3.put(PERSON_F1_NAME, "val3f1");
    val3.put(PERSON_F2_NAME, 30);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key3, val3);
    validatePersonV1Record(storeName, dc0RouterUrl, key3, "val3f1", 30);
    validatePersonV1Record(storeName, dc1RouterUrl, key3, "val3f1", 30);

    // update value schema that removes one old field adds one new field
    assertCommand(parentControllerClient.addValueSchema(storeName, valueSchemaV2.toString()));

    UpdateBuilder ubKv3 = new UpdateBuilderImpl(wcSchemaV2);
    ubKv3.setNewFieldValue(PERSON_F3_NAME, "val3f3");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key3, ubKv3.build());
    validatePersonV1V2SupersetRecord(storeName, dc0RouterUrl, key3, "val3f1", 30, "val3f3");
    validatePersonV1V2SupersetRecord(storeName, dc1RouterUrl, key3, "val3f1", 30, "val3f3");

    // Removed field, when not set should, have a default value
    String key3Prime = "key3Prime";
    GenericRecord val3Prime = new GenericData.Record(valueSchemaV2);
    val3Prime.put(PERSON_F1_NAME, "val3PrimeF1");
    val3Prime.put(PERSON_F3_NAME, "val3PrimeF3");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key3Prime, val3Prime);
    validatePersonV1V2SupersetRecord(storeName, dc0RouterUrl, key3Prime, "val3PrimeF1", -1, "val3PrimeF3");
    validatePersonV1V2SupersetRecord(storeName, dc1RouterUrl, key3Prime, "val3PrimeF1", -1, "val3PrimeF3");
  }

  private void validatePersonV1V2SupersetRecord(
      String storeName,
      String routerUrl,
      String key,
      String expectedField1,
      Integer expectedField2,
      String expectedField3) {
    AvroGenericStoreClient<String, GenericRecord> client = getStoreClient(storeName, routerUrl);
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
      GenericRecord retrievedValue = client.get(key).get();
      if (expectedField1 == null && expectedField2 == null && expectedField3 == null) {
        assertNull(retrievedValue);
        return;
      }
      assertNotNull(retrievedValue);
      if (expectedField1 == null) {
        assertNull(retrievedValue.get(PERSON_F1_NAME));
      } else {
        assertNotNull(retrievedValue.get(PERSON_F1_NAME));
        assertEquals(retrievedValue.get(PERSON_F1_NAME).toString(), expectedField1);
      }
      if (expectedField2 == null) {
        assertNull(retrievedValue.get(PERSON_F2_NAME));
      } else {
        assertNotNull(retrievedValue.get(PERSON_F2_NAME));
        assertEquals(retrievedValue.get(PERSON_F2_NAME), expectedField2);
      }
      if (expectedField3 == null) {
        assertNull(retrievedValue.get(PERSON_F3_NAME));
      } else {
        assertNotNull(retrievedValue.get(PERSON_F3_NAME));
        assertEquals(retrievedValue.get(PERSON_F3_NAME).toString(), expectedField3);
      }
    });
  }

  private void validatePersonV1Record(
      String storeName,
      String routerUrl,
      String key,
      String expectedName,
      Integer expectedAge) {
    AvroGenericStoreClient<String, GenericRecord> client = getStoreClient(storeName, routerUrl);
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
      GenericRecord retrievedValue = client.get(key).get();
      if (expectedName == null && expectedAge == null) {
        assertNull(retrievedValue);
        return;
      }
      assertNotNull(retrievedValue);
      if (expectedName == null) {
        assertNull(retrievedValue.get(PERSON_F1_NAME));
      } else {
        assertNotNull(retrievedValue.get(PERSON_F1_NAME));
        assertEquals(retrievedValue.get(PERSON_F1_NAME).toString(), expectedName);
      }
      if (expectedAge == null) {
        assertNull(retrievedValue.get(PERSON_F2_NAME));
      } else {
        assertNotNull(retrievedValue.get(PERSON_F2_NAME));
        assertEquals(retrievedValue.get(PERSON_F2_NAME), expectedAge);
      }
    });
  }

  private AvroGenericStoreClient<String, GenericRecord> getStoreClient(String storeName, String routerUrl) {
    AvroGenericStoreClient<String, GenericRecord> storeClient = storeClients.get(routerUrl);
    if (storeClient == null) {
      storeClients.put(
          routerUrl,
          ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl)));
    }
    return storeClients.get(routerUrl);
  }

  /*
   * Verify write-compute/partial-update on Map-field level in Active-Active replication setup
   */
  @Test(timeOut = TEST_TIMEOUT, invocationCount = 1)
  public void testAAReplicationForPartialUpdateOnMapField() throws IOException {
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/TestRecordWithFLM.avsc"));
    String regularFieldDefault = "default_venice";
    List<Integer> intArrayFieldDefault = Collections.emptyList();
    Map<String, Integer> mapFieldDefault = Collections.emptyMap();
    Schema wcSchemaV1 = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV1);

    assertCommand(parentControllerClient.createNewStore(storeName, "owner", KEY_SCHEMA_STR, valueSchemaV1.toString()));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
        .setActiveActiveReplicationEnabled(true)
        .setLeaderFollowerModel(true)
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setChunkingEnabled(false)
        .setIncrementalPushEnabled(true)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setWriteComputationEnabled(true);
    assertCommand(parentControllerClient.updateStore(storeName, params));

    runEmptyPushAndVerifyStoreVersion(storeName, 1);
    startVeniceSystemProducers();

    // PART A: Tests without logical timestamp
    // Put: add a full record
    String key1 = "key1";
    String regularFieldValue = "Foo";
    List<Integer> arrayFieldValue = Arrays.asList(11, 22, 33);
    Map<String, Integer> mapFieldValue = new HashMap<>();
    mapFieldValue.put("one", 1);
    mapFieldValue.put("two", 2);
    mapFieldValue.put("three", 3);
    GenericRecord val1 = new GenericData.Record(valueSchemaV1);
    val1.put(REGULAR_FIELD, regularFieldValue);
    val1.put(INT_ARRAY_FIELD, arrayFieldValue);
    val1.put(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);

    UpdateBuilder ub;

    // AddToMap
    Map<String, Integer> updateToMapField = new HashMap<>();
    updateToMapField.put("four", 4);
    updateToMapField.put("five", 5);
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setEntriesToAddToMapField(MAP_FIELD, updateToMapField);
    mapFieldValue.putAll(updateToMapField);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);

    // RemoveFromMap
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("one", "two"));
    mapFieldValue.remove("one");
    mapFieldValue.remove("two");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);

    // AddToMap: add a previously deleted key in the map
    // RemoveFromMap: delete existing and not-existing key
    // Adding and removing the same key in a single wc op should not change the map
    ub = new UpdateBuilderImpl(wcSchemaV1);
    updateToMapField = new HashMap<>();
    updateToMapField.put("one", 11);
    updateToMapField.put("six", 6);
    updateToMapField.put("seven", 7);
    ub.setEntriesToAddToMapField(MAP_FIELD, updateToMapField);
    ub.setKeysToRemoveFromMapField("MapField", Arrays.asList("five", "seven", "eight"));
    mapFieldValue.putAll(updateToMapField);
    mapFieldValue.remove("five");
    mapFieldValue.remove("seven");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);

    // PartialPut: set the value of map field; all old entries will be replaced with new ones as the partialPut
    // will have a higher timestamp.
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("seven", 7);
    mapFieldValue.put("eight", 8);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);

    // PartialPut: set map field to an empty map
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setNewFieldValue(MAP_FIELD, Collections.emptyMap());
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, Collections.emptyMap());
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, Collections.emptyMap());

    // PartialPut with some old entries again
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("seven", 7);
    mapFieldValue.put("eight", 8);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, arrayFieldValue, mapFieldValue);

    // PUT after DELETE: should be able to add back previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("OneNotOne", 101);
    mapFieldValue.put("OneNotTwo", 102);
    mapFieldValue.put("OneNotThree", 103);
    val1 = new GenericData.Record(valueSchemaV1);
    val1.put(REGULAR_FIELD, regularFieldValue);
    val1.put(INT_ARRAY_FIELD, Collections.emptyList());
    val1.put(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, Collections.emptyList(), mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, Collections.emptyList(), mapFieldValue);

    // PartialPut after DELETE: should be able to do PartialPut on the previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("ten", 10);
    mapFieldValue.put("twenty", 20);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldValue);

    // AddToMap after DELETE: should be able to add a key to a map field of the previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("thirty", 30);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldValue);

    // RemoveFromMap after DELETE: should get a record with default values
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Collections.singletonList("fortyTwo"));
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldDefault);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldDefault);

    /* PART B: Tests with logical timestamp
     * The following tests use logical timestamp to simulate various scenarios.
     */
    String key2 = "key2";
    VeniceObjectWithTimestamp timestampedOp;
    Map<String, Integer> expectedMapFieldValue = new HashMap<>();

    // AddToMap before PUT
    // Add three elements to the Map with TS lower than, equal, and greater than the PUT's timestamp
    ub = new UpdateBuilderImpl(wcSchemaV1); // t1
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("OneZero", 10);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldDefault, intArrayFieldDefault, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldDefault, intArrayFieldDefault, expectedMapFieldValue);
    ub = new UpdateBuilderImpl(wcSchemaV1); // t2
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("TwoZero", 20);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldDefault, intArrayFieldDefault, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldDefault, intArrayFieldDefault, expectedMapFieldValue);
    ub = new UpdateBuilderImpl(wcSchemaV1); // t3
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("ThreeZero", 30);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldDefault, intArrayFieldDefault, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldDefault, intArrayFieldDefault, expectedMapFieldValue);

    expectedMapFieldValue.remove("OneZero");
    expectedMapFieldValue.remove("TwoZero");

    // PUT
    // Should keep elements with higher timestamps only
    regularFieldValue = "Key2F1";
    arrayFieldValue = Collections.emptyList();
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("one", 1);
    mapFieldValue.put("two", 2);
    mapFieldValue.put("three", 3);
    GenericRecord val2 = new GenericData.Record(valueSchemaV1);
    val2.put(REGULAR_FIELD, regularFieldValue);
    val2.put(INT_ARRAY_FIELD, arrayFieldValue);
    val2.put(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(val2, 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // AddToMap: Update should be applied since the lowest timestamp of elements in the map(T2) <= timestamp of WC op
    // (t3)
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("four", 4);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("five", 5);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 5);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // todo: We need to have tie-breaking mechanism based on value when timestamps of modifications are the same
    // AddToMap: Key four's current (AddToMap) timestamp is 3 and the following operations is trying to add a new
    // value for the key four again. Currently this second update is ignored. However, tie breaking should not be
    // first come first server based as that leads to non-deterministic output. To keep this consistent with our
    // approach we should use value-based tie breaking in case of concurrent AddToMap for the same key with the same
    // timestamp
    /*
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("four", 40);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    */

    // AddToMap: Update should be ignored since as PUT takes precedence over partial update when timestamps are the same
    // Let's try for the k-v which existed before full-PUT
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("TwoZero", 20);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    regularFieldValue = "TwoZero"; // to make sure that the assertion doesn't go ahead with false-positive
    ub.setNewFieldValue(REGULAR_FIELD, regularFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // AddToMap: Update should be ignored since as PUT takes precedence over partial update with when timestamps are the
    // same
    // Let's try for the k-v which didn't exist before full-PUT
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("TwoZeroTwo", 202);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    regularFieldValue = "TwoZeroTwo";
    ub.setNewFieldValue(REGULAR_FIELD, regularFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    val2 = new GenericData.Record(valueSchemaV1);
    val2.put(REGULAR_FIELD, "");
    val2.put(INT_ARRAY_FIELD, Collections.emptyList());
    val2.put(MAP_FIELD, Collections.emptyMap());

    // AddToMap: Update should be ignored since the lowest timestamp of elements in the map(T2) > timestamp of WC op
    // (t1)
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("OneZeroZero", 100);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another record from the same region to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, "marker", val2);
    verifyFLMRecord(storeName, dc1RouterUrl, "marker", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc0RouterUrl, "marker", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // RemoveFromMap:
    // key('one'): Update should be ignored as 'one' was added by PUT and when timestamps are same PUT takes precedence
    // over WC key('four'): Update should be ignored as DELETE has lower timestamp then the current timestamp of four
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("one", "four"));
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    // send another record from the same region/producer to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, "marker1", val2);
    verifyFLMRecord(storeName, dc1RouterUrl, "marker1", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc0RouterUrl, "marker1", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // RemoveFromMap: Update should be applied as DELETE takes precedence when timestamps match
    ub = new UpdateBuilderImpl(wcSchemaV1);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("four", "one"));
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.remove("four");
    expectedMapFieldValue.remove("one");
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // todo: Fix/add a tie breaker for RemoveFromMap and AddToMap for the same key with the same timestamp
    // AddToMap: adding an element that was deleted with the same timestamp should not succeed as DELETE takes
    // precedence. However, in this case that doesn't happen.
    /*
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("four", 404);
    mapFieldValue.put("one", 101);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    regularFieldValue = "AddBack";
    ub.setNewFieldValue(REGULAR_FIELD, regularFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    */

    // PartialPut: When PartialPut should be ignored when timestamp is lower than all the elements
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("six", 6);
    mapFieldValue.put("seven", 7);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another record from the same region/producer to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, "marker1", val2);
    verifyFLMRecord(storeName, dc1RouterUrl, "marker1", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc0RouterUrl, "marker1", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // TODO: When PartialPut with the same timestamp as PUT is processed, it wipes out the data with the
    // timestamp <= t2 including PUTs timestamp. This creates a race between PartialPut and PUT
    /*
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("six", 6);
    mapFieldValue.put("seven", 7);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another record from the same region/producer to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, "marker1", val2);
    verifyFLMRecord(storeName, dc1RouterUrl, "marker1", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc0RouterUrl, "marker1", "", Collections.emptyList(), Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    */

    // PartialPut: elements with <= to the timestamp of PartialPut will be removed and elements from the PartialPut
    // will be added
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("six", 6);
    mapFieldValue.put("seven", 7);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    expectedMapFieldValue.remove("two"); // existing timestamp 2
    expectedMapFieldValue.remove("three"); // existing timestamp 2
    expectedMapFieldValue.remove("ThreeZero"); // existing timestamp 3
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);

    // todo: Two partial puts with the same timestamp doesn't produce deterministic output
    /*
    ub = new UpdateBuilderImpl(wcSchemaV1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("eight", 88);
    mapFieldValue.put("seven", 77);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldValue, arrayFieldValue, expectedMapFieldValue);
    */
  }

  /*
   * Verify write-compute/partial-update on List-field level in Active-Active replication setup
   */
  @Test(timeOut = TEST_TIMEOUT, invocationCount = 1)
  public void testAAReplicationForPartialUpdateOnListField() throws IOException {
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/TestRecordWithFLM.avsc"));
    String regularFieldDefault = "default_venice";
    List<Integer> intArrayFieldDefault = Collections.emptyList();
    Map<String, Integer> mapFieldDefault = Collections.emptyMap();
    Schema wcSchemaV1 = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV1);

    assertCommand(parentControllerClient.createNewStore(storeName, "owner", KEY_SCHEMA_STR, valueSchemaV1.toString()));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
        .setActiveActiveReplicationEnabled(true)
        .setLeaderFollowerModel(true)
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setChunkingEnabled(false)
        .setIncrementalPushEnabled(true)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setWriteComputationEnabled(true);
    assertCommand(parentControllerClient.updateStore(storeName, params));

    runEmptyPushAndVerifyStoreVersion(storeName, 1);
    startVeniceSystemProducers();

    UpdateBuilder ubV1;

    // Put: add a full record
    String key1 = "key1";
    String regularFieldValue = "Foo";
    Map<String, Integer> mapFieldValue = new HashMap<>();
    mapFieldValue.put("one", 1);
    mapFieldValue.put("two", 2);
    mapFieldValue.put("three", 3);
    List<Integer> expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    GenericRecord val1 = new GenericData.Record(valueSchemaV1);
    val1.put(REGULAR_FIELD, regularFieldValue);
    val1.put(INT_ARRAY_FIELD, expectedArrayFieldVal);
    val1.put(MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    /* PART A: Tests without logical timestamp
     * Since these tests do not provide logical timestamps, timestamp from producer's metadata is used and hence
     * operations that are executed later have higher timestamp than those executed earlier.
     */

    // AddToSet: add two new entries to the list-field
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    List<Integer> addToArrayFieldValue = Arrays.asList(44, 55);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, addToArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 44, 55);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    // RemoveFromSet: remove two entries from the list-field
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    List<Integer> removeFromArrayFieldValue = Arrays.asList(11, 55);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, removeFromArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(22, 33, 44);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    // AddToSet: should be able to add back previously deleted entry;
    // RemoveFromSet: should be able to remove previously removed entry again and non-existing entry
    // Adding and removing the same element in the same operation should not change the elements in the field
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    addToArrayFieldValue = Arrays.asList(11, 66, 77);
    removeFromArrayFieldValue = Arrays.asList(33, 55, 77, 88);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, addToArrayFieldValue);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, removeFromArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(22, 44, 11, 66);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    // PartialPut: replace the value of set/array field
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    addToArrayFieldValue = Arrays.asList(77, 88, 99);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, addToArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(77, 88, 99);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    // PartialPut: remove all entries from the set
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Collections.emptyList());
    expectedArrayFieldVal = Collections.emptyList();
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    // PartialPut: set the value of set/array field
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Arrays.asList(111, 222));
    expectedArrayFieldVal = Arrays.asList(111, 222);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldValue, expectedArrayFieldVal, mapFieldValue);

    // PUT after DELETE: should be able to add back previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    val1 = new GenericData.Record(valueSchemaV1);
    val1.put(REGULAR_FIELD, "");
    val1.put(INT_ARRAY_FIELD, expectedArrayFieldVal);
    val1.put(MAP_FIELD, Collections.emptyMap());
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, val1);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, "", expectedArrayFieldVal, Collections.emptyMap());

    // PartialPut after DELETE: should be able to do PartialPut on the previously deleted Key-Value
    // For a given KEY, a new VALUE should be created with the fields and their values provided in the PartialPut.
    // For remaining fields an empty / (default value) will be used.
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Arrays.asList(77, 88, 99));
    expectedArrayFieldVal = Arrays.asList(77, 88, 99);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldDefault, expectedArrayFieldVal, mapFieldDefault);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldDefault, expectedArrayFieldVal, mapFieldDefault);

    // AddToSet after DELETE: should be able to add elements to a set field of the previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, Arrays.asList(44, 55));
    expectedArrayFieldVal = Arrays.asList(44, 55);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldDefault, expectedArrayFieldVal, mapFieldDefault);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldDefault, expectedArrayFieldVal, mapFieldDefault);

    // RemoveFromSet after DELETE: should get a record with default values
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyFLMRecord(storeName, dc0RouterUrl, key1, null, null, null);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, null, null, null);
    // Key1 was successfully deleted. Now run remove elements op
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, Arrays.asList(44, 55));
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyFLMRecord(storeName, dc0RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldDefault);
    verifyFLMRecord(storeName, dc1RouterUrl, key1, regularFieldDefault, intArrayFieldDefault, mapFieldDefault);

    /* PART B: Tests with logical timestamp
     * The following tests use logical timestamp to simulate various scenarios.
     */

    String key2 = "key2";
    VeniceObjectWithTimestamp timestampedOp;
    // AddToSet is processed before PUT
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, expectedArrayFieldVal);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, regularFieldDefault, expectedArrayFieldVal, mapFieldDefault);
    verifyFLMRecord(storeName, dc1RouterUrl, key2, regularFieldDefault, expectedArrayFieldVal, mapFieldDefault);

    // PutRecord - add [(key, value, timestamp) --> (key2, val2, t1)]
    expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    GenericRecord val2 = new GenericData.Record(valueSchemaV1);
    val2.put(INT_ARRAY_FIELD, expectedArrayFieldVal);
    val2.put(MAP_FIELD, Collections.emptyMap());
    val2.put(REGULAR_FIELD, "");
    timestampedOp = new VeniceObjectWithTimestamp(val2, 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // AddToSet: update with higher timestamp should be applied since t3 > t1
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, Arrays.asList(44, 55));
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 44, 55);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // RemoveFromSet: update with the same timestamp should be applied if incoming operation is DELETE
    // existingEntry: [11:2, 22:2, 33:2, 44:3, 55:3] & incomingUpdate removeFromList[44:3].
    // Here existing and incoming op timestamps for element 44 is the same. As DELETE takes precedence
    // when timestamps match, 44 should be removed from the set.
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, Collections.singletonList(44));
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // AddToSet: adding an element that was deleted with the same timestamp should not succeed as DELETE takes
    // precedence.
    // existingEntry: [11:2, 22:2, 33:2, tombstone(44:3), 55:3] & incomingUpdate addToList[44:3].
    // Here 44 should not be added back since tombstone for it has the same timestamp as that of the incoming op
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    List<Integer> addToListFieldValue = Collections.singletonList(44);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, addToListFieldValue);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // RemoveFromSet: update with lower timestamp should be ignored.
    // Incoming op has timestamp 2 for removeFromList(55) but its timestamp in the entry is 3.
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, Collections.singletonList(55));
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another update from the same region to ensure that previous update was indeed ignored
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToAddToListField(INT_ARRAY_FIELD, Collections.singletonList(66));
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55, 66);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // currentState: [11:2, 22:2, 33:2, tombstone(44:3), 55:3, 66:4]
    // PartialPut: When PartialPut has lower timestamp than all the elements in the list, it should be ignored
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Collections.singletonList(77));
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55, 66);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // PartialPut: When PartialPut has a timestamp which is neither lower nor greater than the timestamps of elements
    // present in the list. Elements with the <= timestamp than the partialPut's timestamp will be removed and
    // elements from the partialPut will be merged with the remaining elements.
    // currentState: [11:2, 22:2, 33:2, tombstone(44:3), 55:3, 66:4] & incomingPartialPut: [11:3, 77:3: 88:3]
    // resultantState: [tombstone(22:2), tombstone(33:2), 11:3, tombstone(44:3), tombstone(44:5), 77:3, 88:3, 66:4]
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Arrays.asList(11, 77, 88));
    expectedArrayFieldVal = Arrays.asList(11, 77, 88, 66);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // Now 11's timestamp should be 3 and if we try to remove it with ts:2 it should not succeed
    // RemoveFromSet: Should be ignored when timestamp is lower than all the elements.
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, Arrays.asList(11, 22, 77, 88, 66));
    expectedArrayFieldVal = Arrays.asList(11, 77, 88, 66);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setElementsToRemoveFromListField(INT_ARRAY_FIELD, Arrays.asList(66));
    expectedArrayFieldVal = Arrays.asList(11, 77, 88);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // The deleted element with the same timestamp should not be added back with PartialPut
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Arrays.asList(66, 99));
    expectedArrayFieldVal = Arrays.asList(99);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());

    // PartialPut: When PartialPut has higher timestamp than all the elements in the list, all existing elements
    // should be replaced with elements in PartialPut.
    ubV1 = new UpdateBuilderImpl(wcSchemaV1);
    ubV1.setNewFieldValue(INT_ARRAY_FIELD, Arrays.asList(101, 202, 303));
    expectedArrayFieldVal = Arrays.asList(101, 202, 303);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 5);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyFLMRecord(storeName, dc0RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
    verifyFLMRecord(storeName, dc1RouterUrl, key2, "", expectedArrayFieldVal, Collections.emptyMap());
  }

  private void verifyFLMRecord(
      String storeName,
      String routerUrl,
      String key,
      String expectedValueOfRegularField,
      List<Integer> expectedValueOfArrayField,
      Map<String, Integer> expectedValueOfMapField) {
    AvroGenericStoreClient<String, GenericRecord> client = getStoreClient(storeName, routerUrl);
    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
      GenericRecord retrievedValue = client.get(key).get();
      if (expectedValueOfRegularField == null && expectedValueOfMapField == null && expectedValueOfArrayField == null) {
        assertNull(retrievedValue);
        return;
      }

      assertNotNull(retrievedValue);

      if (expectedValueOfRegularField == null) {
        assertNull(retrievedValue.get(REGULAR_FIELD));
      } else {
        assertNotNull(retrievedValue.get(REGULAR_FIELD));
        assertEquals(retrievedValue.get(REGULAR_FIELD).toString(), expectedValueOfRegularField);
      }

      if (expectedValueOfArrayField == null) {
        assertNull(retrievedValue.get(INT_ARRAY_FIELD));
      } else {
        assertNotNull(retrievedValue.get(INT_ARRAY_FIELD));
        System.out.println("Expected:" + (List<Integer>) retrievedValue.get(INT_ARRAY_FIELD));
        assertEquals((List<Integer>) retrievedValue.get(INT_ARRAY_FIELD), expectedValueOfArrayField);
      }

      if (expectedValueOfMapField == null) {
        assertNull(retrievedValue.get(MAP_FIELD));
      } else {
        assertNotNull(retrievedValue.get(MAP_FIELD));
        Map<Utf8, Integer> actualValueOfMapField = (HashMap<Utf8, Integer>) retrievedValue.get(MAP_FIELD);
        System.out.println("E: " + expectedValueOfMapField + " A: " + actualValueOfMapField);
        assertEquals(actualValueOfMapField.size(), expectedValueOfMapField.size());
        for (Map.Entry<String, Integer> entry: expectedValueOfMapField.entrySet()) {
          assertEquals(
              actualValueOfMapField.get(new Utf8(entry.getKey())),
              entry.getValue(),
              "Value of key:" + entry.getKey() + " does not match in MapField");
        }
      }
    });
  }
}
