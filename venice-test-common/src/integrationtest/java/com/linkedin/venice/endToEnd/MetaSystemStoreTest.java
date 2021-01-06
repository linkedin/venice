package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static org.testng.Assert.*;


public class MetaSystemStoreTest {
  private static final Logger LOGGER = Logger.getLogger(MetaSystemStoreTest.class);
  private final static String INT_KEY_SCHEMA = "\"int\"";

  private final static String VALUE_SCHEMA_1 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"}\n" + "  ]\n" + "}";
  private final static String VALUE_SCHEMA_2 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"},\n"
      + "   {\"name\": \"test_field2\", \"type\": \"int\", \"default\": 0}\n" + "  ]\n" + "}";

  private VeniceClusterWrapper venice;
  private ControllerClient controllerClient;


  @BeforeClass
  public void setup() {
    venice = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 1000000, false, false);
    controllerClient = venice.getControllerClient();
  }

  @AfterClass
  public void cleanup() {
    controllerClient.close();
    venice.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void bootstrapMetaSystemStore() throws ExecutionException, InterruptedException {
    // Create a new regular store.
    String regularVeniceStoreName = TestUtils.getUniqueString("venice_store");
    NewStoreResponse
        newStoreResponse = controllerClient.createNewStore(regularVeniceStoreName, "test_owner", INT_KEY_SCHEMA, VALUE_SCHEMA_1);
    assertFalse(newStoreResponse.isError(), "New store: " + regularVeniceStoreName + " should be created successfully, but got error: " + newStoreResponse.getError());
    // Do an empty push
    VersionCreationResponse
        versionCreationResponse = controllerClient.emptyPush(regularVeniceStoreName, "test_push_id_1", 100000);
    assertFalse(versionCreationResponse.isError(), "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponse.getKafkaTopic(), controllerClient, 10,
        TimeUnit.SECONDS, Optional.of(LOGGER));
    // Enabling meta system store by triggering an empty push to the corresponding meta system store
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(regularVeniceStoreName);
    VersionCreationResponse versionCreationResponseForMetaSystemStore = controllerClient.emptyPush(metaSystemStoreName,
        "test_meta_system_store_push_1", 10000);
    assertFalse(versionCreationResponseForMetaSystemStore.isError(), "New version creation for meta system store: "
        + metaSystemStoreName + " should success, but got error: " + versionCreationResponseForMetaSystemStore.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponseForMetaSystemStore.getKafkaTopic(), controllerClient, 30,
        TimeUnit.SECONDS, Optional.of(LOGGER));

    // Do another push
    versionCreationResponseForMetaSystemStore = controllerClient.emptyPush(metaSystemStoreName,
        "test_meta_system_store_push_2", 10000);
    assertFalse(versionCreationResponseForMetaSystemStore.isError(), "New version creation for meta system store: "
        + metaSystemStoreName + " should success, but got error: " + versionCreationResponseForMetaSystemStore.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponseForMetaSystemStore.getKafkaTopic(), controllerClient, 30,
        TimeUnit.SECONDS, Optional.of(LOGGER));

    // Query meta system store
    AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> storeClient = ClientFactory.getAndStartSpecificAvroClient(
        ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
        .setVeniceURL(venice.getRandomRouterURL())
        .setSslEngineComponentFactory(SslUtils.getLocalSslFactory())
    );
    // Query store properties
    StoreMetaKey storePropertiesKey = MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(
        new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, regularVeniceStoreName); put(KEY_STRING_CLUSTER_NAME, venice.getClusterName()); }}
        );
    StoreMetaValue storeProperties = storeClient.get(storePropertiesKey).get();
    assertTrue(storeProperties != null && storeProperties.storeProperties != null);
    // Query key schema
    StoreMetaKey keySchemaKey = MetaStoreDataType.STORE_KEY_SCHEMAS.getStoreMetaKey(
        new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, regularVeniceStoreName); put(KEY_STRING_CLUSTER_NAME, venice.getClusterName()); }}
    );
    StoreMetaValue storeKeySchema = storeClient.get(keySchemaKey).get();
    assertTrue(storeKeySchema != null && storeKeySchema.storeKeySchemas != null);
    StoreKeySchemas keySchemas = storeKeySchema.storeKeySchemas;
    assertEquals(keySchemas.keySchemaMap.size(), 1);
    assertEquals(keySchemas.keySchemaMap.get(new Utf8("1")).toString(), INT_KEY_SCHEMA);
    // Query value schema
    StoreMetaKey valueSchemasKey = MetaStoreDataType.STORE_VALUE_SCHEMAS.getStoreMetaKey(
        new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, regularVeniceStoreName); put(KEY_STRING_CLUSTER_NAME, venice.getClusterName()); }}
    );
    StoreMetaValue storeValueSchemas = storeClient.get(valueSchemasKey).get();
    assertTrue(storeValueSchemas != null && storeValueSchemas.storeValueSchemas != null);
    assertEquals(storeValueSchemas.storeValueSchemas.valueSchemaMap.size(), 1);
    assertEquals(Schema.parse(storeValueSchemas.storeValueSchemas.valueSchemaMap.get(new Utf8("1")).toString()), Schema.parse(VALUE_SCHEMA_1));
    // Query replica status
    StoreMetaKey replicaStatusKeyForV1P0 = MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(
        new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
          put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
          put(KEY_STRING_VERSION_NUMBER, "1");
          put(KEY_STRING_PARTITION_ID, "0");
        }}
    );
    StoreMetaValue replicaStatusForV1P0 = storeClient.get(replicaStatusKeyForV1P0).get();
    // the different situations.
    assertTrue(replicaStatusForV1P0 != null && replicaStatusForV1P0.storeReplicaStatuses != null);

    // Update store config
    controllerClient.updateStore(regularVeniceStoreName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    // Query meta system store to verify the change
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      final StoreMetaValue v = storeClient.get(storePropertiesKey).get();
      assertEquals(v.storeProperties.batchGetLimit, 100);
    });

    // Add a new value schema
    controllerClient.addValueSchema(regularVeniceStoreName, VALUE_SCHEMA_2);
    // Query meta system store to verify the change
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      final StoreMetaValue v = storeClient.get(valueSchemasKey).get();
      assertEquals(v.storeValueSchemas.valueSchemaMap.size(), 2);
      assertEquals(Schema.parse(v.storeValueSchemas.valueSchemaMap.get(new Utf8("2")).toString()),
          Schema.parse(VALUE_SCHEMA_2));
    });

    // Do the 2nd empty push to the Venice store
    // Do an empty push
    versionCreationResponse = controllerClient.emptyPush(regularVeniceStoreName, "test_push_id_2", 100000);
    assertFalse(versionCreationResponse.isError(), "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponse.getKafkaTopic(), controllerClient, 10000,
        TimeUnit.SECONDS, Optional.of(LOGGER));
    // Query replica status
    StoreMetaKey replicaStatusKeyForV2P0 = MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(
        new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
          put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
          put(KEY_STRING_VERSION_NUMBER, "2");
          put(KEY_STRING_PARTITION_ID, "0");
        }}
    );
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreMetaValue replicaStatusForV2P0 = storeClient.get(replicaStatusKeyForV2P0).get();
      assertNotNull(replicaStatusForV2P0);
      if (replicaStatusForV2P0 != null) {
        assertEquals(replicaStatusForV2P0.storeReplicaStatuses.size(), 2);
      }
    });

    // Do the 3rd empty push to the Venice store, and the replica status for 1st version should become empty
    versionCreationResponse = controllerClient.emptyPush(regularVeniceStoreName, "test_push_id_3", 100000);
    assertFalse(versionCreationResponse.isError(), "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponse.getKafkaTopic(), controllerClient, 10,
        TimeUnit.SECONDS, Optional.of(LOGGER));
    // Query replica status
    StoreMetaKey replicaStatusKeyForV3P0 = MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(
        new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
          put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
          put(KEY_STRING_VERSION_NUMBER, "3");
          put(KEY_STRING_PARTITION_ID, "0");
        }}
    );
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreMetaValue replicaStatusForV3P0 = storeClient.get(replicaStatusKeyForV3P0).get();
      assertNotNull(replicaStatusForV3P0);
      if (replicaStatusForV3P0 != null) {
        assertEquals(replicaStatusForV3P0.storeReplicaStatuses.size(), 2);
      }

      /**
       * Since we don't have logic to remove the entries for the deprecated versions right now,
       * the corresponding replica status should contain an emtpy map.
       */
      StoreMetaValue currentReplicaStatusForV1P0 = storeClient.get(replicaStatusKeyForV1P0).get();
      assertNotNull(currentReplicaStatusForV1P0);
      assertEquals(currentReplicaStatusForV1P0.storeReplicaStatuses.size(), 0);
    });
  }
}
