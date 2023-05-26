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
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
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
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestPartialUpdateWithActiveActiveReplication {
  private static final Logger LOGGER = LogManager.getLogger(TestPartialUpdateWithActiveActiveReplication.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final int PUSH_TIMEOUT = TEST_TIMEOUT / 2;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  public static final String REGULAR_FIELD = "regularField";
  public static final String LIST_FIELD = "listField";
  public static final String NULLABLE_LIST_FIELD = "nullableListField";
  public static final String MAP_FIELD = "mapField";
  public static final String NULLABLE_MAP_FIELD = "nullableMapField";

  private static final Map<String, Integer> MAP_FIELD_DEFAULT_VALUE = Collections.emptyMap();
  private static final Map<String, Integer> NULLABLE_MAP_FIELD_DEFAULT_VALUE = null;
  private static final List<Integer> LIST_FIELD_DEFAULT_VALUE = Collections.emptyList();
  private static final List<Integer> NULLABLE_LIST_FIELD_DEFAULT_VALUE = null;

  private static final String REGULAR_FIELD_DEFAULT_VALUE = "default_venice";

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;
  private ControllerClient dc0Client;
  private ControllerClient dc1Client;
  private List<ControllerClient> dcControllerClientList;
  private String dc0RouterUrl;
  private String dc1RouterUrl;

  private final static String KEY_SCHEMA_STR = "{\"type\" : \"string\"}";
  private final static String PERSON_F1_NAME = "name";
  private final static String PERSON_F2_NAME = "age";
  private final static String PERSON_F3_NAME = "hometown";

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

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
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

    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();

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
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @BeforeMethod
  public void setUpStore() {
    storeName = Utils.getUniqueString("test-store-aa-wc");
    storeClients = new HashMap<>(2);
  }

  @AfterMethod
  public void cleanUpAfterMethod() {
    try {
      parentControllerClient.disableAndDeleteStore(storeName);
    } catch (Exception e) {
      // ignore... this is just best-effort.
      LOGGER.info("Failed to delete the store during cleanup with message: {}", e.getLocalizedMessage());
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

  // Create one system producer per region
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
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress());
      samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
      samzaConfig.put(SSL_ENABLED, "false");
      VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();
      systemProducerMap.put(childDataCenter, veniceProducer);
    }
  }

  /*
   * Verify partial-update on field level in Active-Active replication setup
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
    return storeClients.computeIfAbsent(
        routerUrl,
        k -> ClientFactory
            .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl)));
  }

  /*
   * Verify partial-update on Map-field level in Active-Active replication setup
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAAReplicationForPartialUpdateOnMapField() throws IOException {
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("PartialUpdateMapField.avsc"));
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    assertCommand(parentControllerClient.createNewStore(storeName, "owner", KEY_SCHEMA_STR, valueSchema.toString()));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
        .setActiveActiveReplicationEnabled(true)
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setChunkingEnabled(true)
        .setIncrementalPushEnabled(true)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setWriteComputationEnabled(true);
    assertCommand(parentControllerClient.updateStore(storeName, params));

    runEmptyPushAndVerifyStoreVersion(storeName, 1);
    startVeniceSystemProducers();
    UpdateBuilder ub;

    // PART A: Tests without logical timestamp
    // Put: add a full record
    String key1 = "key1";
    String regularFieldValue = "Foo";
    Map<String, Integer> mapFieldValue = new HashMap<>();
    mapFieldValue.put("one", 1);
    mapFieldValue.put("two", 2);
    mapFieldValue.put("three", 3);
    GenericRecord val1 = new GenericData.Record(valueSchema);
    val1.put(REGULAR_FIELD, regularFieldValue);
    val1.put(MAP_FIELD, mapFieldValue);
    val1.put(NULLABLE_MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // AddToMap
    Map<String, Integer> updateToMapField = new HashMap<>();
    updateToMapField.put("four", 4);
    updateToMapField.put("five", 5);
    ub = new UpdateBuilderImpl(updateSchema);
    ub.setEntriesToAddToMapField(MAP_FIELD, updateToMapField);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, updateToMapField);
    mapFieldValue.putAll(updateToMapField);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // RemoveFromMap
    ub = new UpdateBuilderImpl(updateSchema);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("one", "two"));
    ub.setKeysToRemoveFromMapField(NULLABLE_MAP_FIELD, Arrays.asList("one", "two"));
    mapFieldValue.remove("one");
    mapFieldValue.remove("two");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // AddToMap: add a previously deleted key in the map
    // RemoveFromMap: delete existing and not-existing key
    // Adding and removing the same key in a single wc op should not change the map
    ub = new UpdateBuilderImpl(updateSchema);
    updateToMapField = new HashMap<>();
    updateToMapField.put("one", 11);
    updateToMapField.put("six", 6);
    updateToMapField.put("seven", 7);
    ub.setEntriesToAddToMapField(MAP_FIELD, updateToMapField);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, updateToMapField);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("five", "seven", "eight"));
    ub.setKeysToRemoveFromMapField(NULLABLE_MAP_FIELD, Arrays.asList("five", "seven", "eight"));
    mapFieldValue.putAll(updateToMapField);
    mapFieldValue.remove("five");
    mapFieldValue.remove("seven");
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // PartialPut: set the value of map field; all old entries will be replaced with new ones as the partialPut
    // will have a higher timestamp.
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("seven", 7);
    mapFieldValue.put("eight", 8);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // PartialPut: set map field to an empty map
    ub = new UpdateBuilderImpl(updateSchema);
    ub.setNewFieldValue(MAP_FIELD, Collections.emptyMap());
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, Collections.emptyMap());
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        Collections.emptyMap(),
        Collections.emptyMap());

    // PartialPut with some old entries again
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("seven", 7);
    mapFieldValue.put("eight", 8);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // PUT after DELETE: should be able to add back previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("OneNotOne", 101);
    mapFieldValue.put("OneNotTwo", 102);
    mapFieldValue.put("OneNotThree", 103);
    val1 = new GenericData.Record(valueSchema);
    val1.put(REGULAR_FIELD, regularFieldValue);
    val1.put(MAP_FIELD, mapFieldValue);
    val1.put(NULLABLE_MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    verifyMapFieldsValueForAllRegions(storeName, key1, regularFieldValue, mapFieldValue, mapFieldValue);

    // PartialPut after DELETE: should be able to do PartialPut on the previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("ten", 10);
    mapFieldValue.put("twenty", 20);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, REGULAR_FIELD_DEFAULT_VALUE, mapFieldValue, mapFieldValue);

    // AddToMap after DELETE: should be able to add a key to a map field of the previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("thirty", 30);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(storeName, key1, REGULAR_FIELD_DEFAULT_VALUE, mapFieldValue, mapFieldValue);

    // RemoveFromMap after DELETE: should get a record with default values
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);
    ub = new UpdateBuilderImpl(updateSchema);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Collections.singletonList("fortyTwo"));
    ub.setKeysToRemoveFromMapField(NULLABLE_MAP_FIELD, Collections.singletonList("fortyTwo"));
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ub.build());
    verifyMapFieldsValueForAllRegions(
        storeName,
        key1,
        REGULAR_FIELD_DEFAULT_VALUE,
        MAP_FIELD_DEFAULT_VALUE,
        MAP_FIELD_DEFAULT_VALUE);

    /* PART B: Tests with logical timestamp
     * The following tests use logical timestamp to simulate various scenarios.
     */

    String key2 = "key2";
    VeniceObjectWithTimestamp timestampedOp;
    Map<String, Integer> expectedMapFieldValue = new HashMap<>();

    // AddToMap before PUT
    // Update on the same key by AddToMap operation should be reflected.
    ub = new UpdateBuilderImpl(updateSchema); // t1
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("xx", 1);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(
        storeName,
        key2,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedMapFieldValue,
        expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema); // t2
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("xx", 2);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(
        storeName,
        key2,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedMapFieldValue,
        expectedMapFieldValue);

    // Add three elements to the Map with TS lower than, equal, and greater than the PUT's timestamp
    ub = new UpdateBuilderImpl(updateSchema); // t1
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("OneZero", 10);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(
        storeName,
        key2,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedMapFieldValue,
        expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema); // t2
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("TwoZero", 20);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(
        storeName,
        key2,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedMapFieldValue,
        expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema); // t3
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("ThreeZero", 30);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    expectedMapFieldValue.putAll(mapFieldValue);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(
        storeName,
        key2,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedMapFieldValue,
        expectedMapFieldValue);

    // PUT
    // Should keep elements with higher timestamps only
    regularFieldValue = "Key2F1";
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("one", 1);
    mapFieldValue.put("two", 2);
    mapFieldValue.put("three", 3);
    GenericRecord val2 = new GenericData.Record(valueSchema);
    val2.put(REGULAR_FIELD, regularFieldValue);
    val2.put(MAP_FIELD, mapFieldValue);
    val2.put(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(val2, 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    expectedMapFieldValue.remove("OneZero");
    expectedMapFieldValue.remove("TwoZero");
    expectedMapFieldValue.remove("xx");
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // AddToMap: Update should be applied since the lowest timestamp of elements in the map(T2) <= timestamp of WC op
    // (t3)
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("four", 4);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("five", 5);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 5);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // AddToMap: Key four's current (AddToMap) timestamp is 3 and the following operations is trying to add a new
    // value for the key four again. Currently this second update is ignored. However, tie breaking should not be
    // first come first server based as that leads to non-deterministic output. To keep this consistent with our
    // approach we should use value-based tie breaking in case of concurrent AddToMap for the same key with the same
    // timestamp
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("four", 40);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // AddToMap: Update should be ignored since as PUT takes precedence over partial update when timestamps are the same
    // Let's try for the k-v which existed before full-PUT
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("TwoZero", 20);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    regularFieldValue = "TwoZero"; // to make sure that the assertion doesn't go ahead with false-positive
    ub.setNewFieldValue(REGULAR_FIELD, regularFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // AddToMap: Update should be ignored since as PUT takes precedence over partial update with when timestamps are the
    // same
    // Let's try for the k-v which didn't exist before full-PUT
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("TwoZeroTwo", 202);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    regularFieldValue = "TwoZeroTwo";
    ub.setNewFieldValue(REGULAR_FIELD, regularFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    val2 = new GenericData.Record(valueSchema);
    val2.put(REGULAR_FIELD, "");
    val2.put(MAP_FIELD, Collections.emptyMap());
    val2.put(NULLABLE_MAP_FIELD, Collections.emptyMap());

    // AddToMap: Update should be ignored since the lowest timestamp of elements in the map(T2) > timestamp of WC op
    // (t1)
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("OneZeroZero", 100);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another record from the same region to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, "marker", val2);
    verifyMapFieldsValueForAllRegions(storeName, "marker", "", Collections.emptyMap(), Collections.emptyMap());
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // RemoveFromMap:
    // key('one'): Update should be ignored as 'one' was added by PUT and when timestamps are same PUT takes precedence
    // over WC key('four'): Update should be ignored as DELETE has lower timestamp then the current timestamp of four
    ub = new UpdateBuilderImpl(updateSchema);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("one", "four"));
    ub.setKeysToRemoveFromMapField(NULLABLE_MAP_FIELD, Arrays.asList("one", "four"));
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    // send another record from the same region/producer to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, "marker1", val2);
    verifyMapFieldsValueForAllRegions(storeName, "marker1", "", Collections.emptyMap(), Collections.emptyMap());
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // RemoveFromMap: Update should be applied as DELETE takes precedence when timestamps match
    ub = new UpdateBuilderImpl(updateSchema);
    ub.setKeysToRemoveFromMapField(MAP_FIELD, Arrays.asList("four", "one"));
    ub.setKeysToRemoveFromMapField(NULLABLE_MAP_FIELD, Arrays.asList("four", "one"));
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.remove("four");
    expectedMapFieldValue.remove("one");
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("four", 404);
    mapFieldValue.put("one", 101);
    ub.setEntriesToAddToMapField(MAP_FIELD, mapFieldValue);
    ub.setEntriesToAddToMapField(NULLABLE_MAP_FIELD, mapFieldValue);
    regularFieldValue = "AddBack";
    ub.setNewFieldValue(REGULAR_FIELD, regularFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // PartialPut: When PartialPut should be ignored when timestamp is lower than all the elements
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("six", 6);
    mapFieldValue.put("seven", 7);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another record from the same region/producer to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, "marker1", val2);
    verifyMapFieldsValueForAllRegions(storeName, "marker1", "", Collections.emptyMap(), Collections.emptyMap());
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("six", 6);
    mapFieldValue.put("seven", 7);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    expectedMapFieldValue.remove("two");
    expectedMapFieldValue.remove("three");

    // send another record from the same region/producer to ensure that the previous operation was completed
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, "marker1", val2);
    verifyMapFieldsValueForAllRegions(storeName, "marker1", "", Collections.emptyMap(), Collections.emptyMap());
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    // PartialPut: elements with <= to the timestamp of PartialPut will be removed and elements from the PartialPut
    // will be added
    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("six", 60);
    mapFieldValue.put("seven", 70);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    expectedMapFieldValue.putAll(mapFieldValue);
    expectedMapFieldValue.remove("two"); // existing timestamp 2
    expectedMapFieldValue.remove("three"); // existing timestamp 2
    expectedMapFieldValue.remove("ThreeZero"); // existing timestamp 3
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

    ub = new UpdateBuilderImpl(updateSchema);
    mapFieldValue = new HashMap<>();
    mapFieldValue.put("eight", 88);
    mapFieldValue.put("seven", 77);
    ub.setNewFieldValue(MAP_FIELD, mapFieldValue);
    ub.setNewFieldValue(NULLABLE_MAP_FIELD, mapFieldValue);
    timestampedOp = new VeniceObjectWithTimestamp(ub.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // This setField UPDATE is ignored, as it has smaller colo ID compared to existing top-level colo ID.
    verifyMapFieldsValueForAllRegions(storeName, key2, regularFieldValue, expectedMapFieldValue, expectedMapFieldValue);

  }

  /*
   * Verify partial-update on List-field level in Active-Active replication setup
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAAReplicationForPartialUpdateOnListField() throws IOException {
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("PartialUpdateListField.avsc"));
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    assertCommand(parentControllerClient.createNewStore(storeName, "owner", KEY_SCHEMA_STR, valueSchema.toString()));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
        .setActiveActiveReplicationEnabled(true)
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
    List<Integer> expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    GenericRecord val1 = new GenericData.Record(valueSchema);
    val1.put(REGULAR_FIELD, regularFieldValue);
    val1.put(LIST_FIELD, expectedArrayFieldVal);
    val1.put(NULLABLE_LIST_FIELD, expectedArrayFieldVal);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, val1);
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    /* PART A: Tests without logical timestamp
     * Since these tests do not provide logical timestamps, timestamp from producer's metadata is used and hence
     * operations that are executed later have higher timestamp than those executed earlier.
     */

    // AddToSet: add two new entries to the list-field
    ubV1 = new UpdateBuilderImpl(updateSchema);
    List<Integer> addToArrayFieldValue = Arrays.asList(44, 55);
    ubV1.setElementsToAddToListField(LIST_FIELD, addToArrayFieldValue);
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, addToArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 44, 55);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // RemoveFromSet: remove two entries from the list-field
    ubV1 = new UpdateBuilderImpl(updateSchema);
    List<Integer> removeFromArrayFieldValue = Arrays.asList(11, 55);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, removeFromArrayFieldValue);
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, removeFromArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(22, 33, 44);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // AddToSet: should be able to add back previously deleted entry;
    // RemoveFromSet: should be able to remove previously removed entry again and non-existing entry
    // Adding and removing the same element in the same operation should not change the elements in the field
    ubV1 = new UpdateBuilderImpl(updateSchema);
    addToArrayFieldValue = Arrays.asList(11, 66, 77);
    removeFromArrayFieldValue = Arrays.asList(33, 55, 77, 88);
    ubV1.setElementsToAddToListField(LIST_FIELD, addToArrayFieldValue);
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, addToArrayFieldValue);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, removeFromArrayFieldValue);
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, removeFromArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(22, 44, 11, 66);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // PartialPut: replace the value of set/array field
    ubV1 = new UpdateBuilderImpl(updateSchema);
    addToArrayFieldValue = Arrays.asList(77, 88, 99);
    ubV1.setNewFieldValue(LIST_FIELD, addToArrayFieldValue);
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, addToArrayFieldValue);
    expectedArrayFieldVal = Arrays.asList(77, 88, 99);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // PartialPut: remove all entries from the set
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Collections.emptyList());
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Collections.emptyList());
    expectedArrayFieldVal = Collections.emptyList();
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // PartialPut: set the value of set/array field
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Arrays.asList(111, 222));
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Arrays.asList(111, 222));
    expectedArrayFieldVal = Arrays.asList(111, 222);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        regularFieldValue,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // PUT after DELETE: should be able to add back previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);

    expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    val1 = new GenericData.Record(valueSchema);
    val1.put(REGULAR_FIELD, "");
    val1.put(LIST_FIELD, expectedArrayFieldVal);
    val1.put(NULLABLE_LIST_FIELD, expectedArrayFieldVal);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, val1);
    verifyListFieldsValueForAllRegions(storeName, key1, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // PartialPut after DELETE: should be able to do PartialPut on the previously deleted Key-Value
    // For a given KEY, a new VALUE should be created with the fields and their values provided in the PartialPut.
    // For remaining fields an empty / (default value) will be used.
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Arrays.asList(77, 88, 99));
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Arrays.asList(77, 88, 99));
    expectedArrayFieldVal = Arrays.asList(77, 88, 99);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // AddToSet after DELETE: should be able to add elements to a set field of the previously deleted Key-Value
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToAddToListField(LIST_FIELD, Arrays.asList(44, 55));
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, Arrays.asList(44, 55));
    expectedArrayFieldVal = Arrays.asList(44, 55);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // RemoveFromSet after DELETE: should get a record with default values
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key1, null);
    verifyNullValueForAllRegions(storeName, key1);

    // Key1 was successfully deleted. Now run remove elements op
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, Arrays.asList(44, 55));
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, Arrays.asList(44, 55));
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key1, ubV1.build());
    verifyListFieldsValueForAllRegions(
        storeName,
        key1,
        REGULAR_FIELD_DEFAULT_VALUE,
        LIST_FIELD_DEFAULT_VALUE,
        LIST_FIELD_DEFAULT_VALUE);

    /* PART B: Tests with logical timestamp
     * The following tests use logical timestamp to simulate various scenarios.
     */
    String key2 = "key2";
    VeniceObjectWithTimestamp timestampedOp;
    // AddToSet is processed before PUT
    ubV1 = new UpdateBuilderImpl(updateSchema);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    ubV1.setElementsToAddToListField(LIST_FIELD, expectedArrayFieldVal);
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, expectedArrayFieldVal);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(
        storeName,
        key2,
        REGULAR_FIELD_DEFAULT_VALUE,
        expectedArrayFieldVal,
        expectedArrayFieldVal);

    // PutRecord - add [(key, value, timestamp) --> (key2, val2, t1)]
    expectedArrayFieldVal = Arrays.asList(11, 22, 33);
    GenericRecord val2 = new GenericData.Record(valueSchema);
    val2.put(LIST_FIELD, expectedArrayFieldVal);
    val2.put(NULLABLE_LIST_FIELD, expectedArrayFieldVal);
    val2.put(REGULAR_FIELD, "");
    timestampedOp = new VeniceObjectWithTimestamp(val2, 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // AddToSet: update with higher timestamp should be applied since t3 > t1
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToAddToListField(LIST_FIELD, Arrays.asList(44, 55));
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, Arrays.asList(44, 55));
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 44, 55);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // RemoveFromSet: update with the same timestamp should be applied if incoming operation is DELETE
    // existingEntry: [11:2, 22:2, 33:2, 44:3, 55:3] & incomingUpdate removeFromList[44:3].
    // Here existing and incoming op timestamps for element 44 is the same. As DELETE takes precedence
    // when timestamps match, 44 should be removed from the set.
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, Collections.singletonList(44));
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, Collections.singletonList(44));
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // AddToSet: adding an element that was deleted with the same timestamp should not succeed as DELETE takes
    // precedence.
    // existingEntry: [11:2, 22:2, 33:2, tombstone(44:3), 55:3] & incomingUpdate addToList[44:3].
    // Here 44 should not be added back since tombstone for it has the same timestamp as that of the incoming op
    ubV1 = new UpdateBuilderImpl(updateSchema);
    List<Integer> addToListFieldValue = Collections.singletonList(44);
    ubV1.setElementsToAddToListField(LIST_FIELD, addToListFieldValue);
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, addToListFieldValue);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // RemoveFromSet: update with lower timestamp should be ignored.
    // Incoming op has timestamp 2 for removeFromList(55) but its timestamp in the entry is 3.
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, Collections.singletonList(55));
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, Collections.singletonList(55));
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    // send another update from the same region to ensure that previous update was indeed ignored
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToAddToListField(LIST_FIELD, Collections.singletonList(66));
    ubV1.setElementsToAddToListField(NULLABLE_LIST_FIELD, Collections.singletonList(66));
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55, 66);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // currentState: [11:2, 22:2, 33:2, tombstone(44:3), 55:3, 66:4]
    // PartialPut: When PartialPut has lower timestamp than all the elements in the list, it should be ignored
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Collections.singletonList(77));
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Collections.singletonList(77));
    expectedArrayFieldVal = Arrays.asList(11, 22, 33, 55, 66);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 1);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // PartialPut: When PartialPut has a timestamp which is neither lower nor greater than the timestamps of elements
    // present in the list. Elements with the <= timestamp than the partialPut's timestamp will be removed and
    // elements from the partialPut will be merged with the remaining elements.
    // currentState: [11:2, 22:2, 33:2, tombstone(44:3), 55:3, 66:4] & incomingPartialPut: [11:3, 77:3: 88:3]
    // resultantState: [tombstone(22:2), tombstone(33:2), 11:3, tombstone(44:3), tombstone(44:5), 77:3, 88:3, 66:4]
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Arrays.asList(11, 77, 88));
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Arrays.asList(11, 77, 88));
    expectedArrayFieldVal = Arrays.asList(11, 77, 88, 66);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 3);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // Now 11's timestamp should be 3 and if we try to remove it with ts:2 it should not succeed
    // RemoveFromSet: Should be ignored when timestamp is lower than all the elements.
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, Arrays.asList(11, 22, 77, 88, 66));
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, Arrays.asList(11, 22, 77, 88, 66));
    expectedArrayFieldVal = Arrays.asList(11, 77, 88, 66);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 2);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setElementsToRemoveFromListField(LIST_FIELD, Collections.singletonList(66));
    ubV1.setElementsToRemoveFromListField(NULLABLE_LIST_FIELD, Collections.singletonList(66));
    expectedArrayFieldVal = Arrays.asList(11, 77, 88);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(1)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // The deleted element with the same timestamp should not be added back with PartialPut
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Arrays.asList(66, 99));
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Arrays.asList(66, 99));
    expectedArrayFieldVal = Collections.singletonList(99);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 4);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);

    // PartialPut: When PartialPut has higher timestamp than all the elements in the list, all existing elements
    // should be replaced with elements in PartialPut.
    ubV1 = new UpdateBuilderImpl(updateSchema);
    ubV1.setNewFieldValue(LIST_FIELD, Arrays.asList(101, 202, 303));
    ubV1.setNewFieldValue(NULLABLE_LIST_FIELD, Arrays.asList(101, 202, 303));
    expectedArrayFieldVal = Arrays.asList(101, 202, 303);
    timestampedOp = new VeniceObjectWithTimestamp(ubV1.build(), 5);
    sendStreamingRecord(systemProducerMap.get(childDatacenters.get(0)), storeName, key2, timestampedOp);
    verifyListFieldsValueForAllRegions(storeName, key2, "", expectedArrayFieldVal, expectedArrayFieldVal);
  }

  private void verifyMapFieldsValueForAllRegions(
      String storeName,
      String key,
      String expectedValueOfRegularField,
      Map<String, Integer> expectedValueOfMapField,
      Map<String, Integer> expectedValueOfNullableMapField) {
    verifyMapFieldPartialUpdate(
        storeName,
        dc0RouterUrl,
        key,
        expectedValueOfRegularField,
        expectedValueOfMapField,
        expectedValueOfNullableMapField);
    verifyMapFieldPartialUpdate(
        storeName,
        dc1RouterUrl,
        key,
        expectedValueOfRegularField,
        expectedValueOfMapField,
        expectedValueOfNullableMapField);
  }

  private void verifyNullValueForAllRegions(String storeName, String key) {
    for (String routerUrl: storeClients.keySet()) {
      AvroGenericStoreClient<String, GenericRecord> client = getStoreClient(storeName, routerUrl);
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
        GenericRecord retrievedValue = client.get(key).get();
        Assert.assertNull(retrievedValue);
      });
    }
  }

  private void verifyMapFieldPartialUpdate(
      String storeName,
      String routerUrl,
      String key,
      String expectedValueOfRegularField,
      Map<String, Integer> expectedValueOfMapField,
      Map<String, Integer> expectedValueOfNullableMapField) {
    AvroGenericStoreClient<String, GenericRecord> client = getStoreClient(storeName, routerUrl);
    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
      GenericRecord retrievedValue = client.get(key).get();
      assertNotNull(retrievedValue);
      if (expectedValueOfRegularField == null) {
        assertNull(retrievedValue.get(REGULAR_FIELD));
      } else {
        assertNotNull(retrievedValue.get(REGULAR_FIELD));
        assertEquals(retrievedValue.get(REGULAR_FIELD).toString(), expectedValueOfRegularField);
      }
      validateMapField(expectedValueOfMapField, (Map<Utf8, Integer>) retrievedValue.get(MAP_FIELD));
      validateMapField(expectedValueOfNullableMapField, (Map<Utf8, Integer>) retrievedValue.get(NULLABLE_MAP_FIELD));
    });
  }

  private void validateMapField(Map<String, Integer> expectedMapValue, Map<Utf8, Integer> actualMapValue) {
    if (expectedMapValue == null) {
      assertNull(actualMapValue);
    } else {
      assertNotNull(actualMapValue);
      System.out.println("Expected value: " + expectedMapValue + " Actual value: " + actualMapValue);
      assertEquals(actualMapValue.size(), expectedMapValue.size());
      for (Map.Entry<String, Integer> entry: expectedMapValue.entrySet()) {
        assertEquals(
            actualMapValue.get(new Utf8(entry.getKey())),
            entry.getValue(),
            "Value of key:" + entry.getKey() + " does not match in field");
      }
    }
  }

  private void verifyListFieldsValueForAllRegions(
      String storeName,
      String key,
      String expectedValueOfRegularField,
      List<Integer> expectedValueOfListField,
      List<Integer> expectedValueOfNullableListField) {
    verifyListFieldPartialUpdate(
        storeName,
        dc0RouterUrl,
        key,
        expectedValueOfRegularField,
        expectedValueOfListField,
        expectedValueOfNullableListField);
    verifyListFieldPartialUpdate(
        storeName,
        dc1RouterUrl,
        key,
        expectedValueOfRegularField,
        expectedValueOfListField,
        expectedValueOfNullableListField);
  }

  private void verifyListFieldPartialUpdate(
      String storeName,
      String routerUrl,
      String key,
      String expectedValueOfRegularField,
      List<Integer> expectedValueOfListField,
      List<Integer> expectedValueOfNullableListField) {
    AvroGenericStoreClient<String, GenericRecord> client = getStoreClient(storeName, routerUrl);
    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
      GenericRecord retrievedValue = client.get(key).get();
      assertNotNull(retrievedValue);
      if (expectedValueOfRegularField == null) {
        assertNull(retrievedValue.get(REGULAR_FIELD));
      } else {
        assertNotNull(retrievedValue.get(REGULAR_FIELD));
        assertEquals(retrievedValue.get(REGULAR_FIELD).toString(), expectedValueOfRegularField);
      }
      validateListField(expectedValueOfListField, (List<Integer>) retrievedValue.get(LIST_FIELD));
      validateListField(expectedValueOfNullableListField, (List<Integer>) retrievedValue.get(NULLABLE_LIST_FIELD));
    });
  }

  private void validateListField(List<Integer> expectedListValue, List<Integer> actualListValue) {
    if (expectedListValue == null) {
      assertNull(actualListValue);
    } else {
      assertNotNull(actualListValue);
      System.out.println("Expected:" + actualListValue);
      assertEquals(actualListValue, expectedListValue);
    }
  }
}
