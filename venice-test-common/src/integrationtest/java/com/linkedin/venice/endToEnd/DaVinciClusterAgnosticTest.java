package com.linkedin.venice.endToEnd;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;

import io.tehuti.metrics.MetricsRepository;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.linkedin.venice.ConfigKeys.*;
import static org.testng.Assert.*;


public class DaVinciClusterAgnosticTest {
  private final static int METADATA_STORE_VERSION = 1;
  private final static String INT_KEY_SCHEMA = "\"int\"";
  private final static String INT_VALUE_SCHEMA = "\"int\"";
  public static final String RECORD_VALUE_SCHEMA = "{" +
      "  \"namespace\": \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"TestRecord\",     " +
      "  \"fields\": [           " +
      "       {\"name\": \"field1\", \"type\": \"int\"}  " +
      "  ] " +
      " } ";
  public static final String NEW_RECORD_VALUE_SCHEMA = "{" +
      "  \"namespace\": \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"TestRecord\",     " +
      "  \"fields\": [           " +
      "       {\"name\": \"field1\", \"type\": \"int\"},  " +
      "       {\"name\": \"field2\", \"type\": \"int\", \"default\": 0}" +
      "  ] " +
      " } ";
  private final static String FABRIC = "dc-0";

  private VeniceMultiClusterWrapper multiClusterVenice;
  private String[] clusterNames;
  private ZkServerWrapper zkServer;
  private VeniceControllerWrapper parentController;
  private D2Client d2Client;

  /**
   * Set up a multi-cluster Venice environment with metadata system store enabled Venice stores.
   */
  @BeforeClass
  public void setup() {
    zkServer = ServiceFactory.getZkServer();
    Properties testProperties = new Properties();
    testProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    testProperties.setProperty(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, "false");
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    testProperties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        String.valueOf(Long.MAX_VALUE));
    testProperties.setProperty(CONTROLLER_DISABLE_PARENT_TOPIC_TRUNCATION_UPON_COMPLETION, Boolean.toString(true));
    d2Client = D2TestUtils.getAndStartD2Client(zkServer.getAddress());
    testProperties.put(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER, ClientConfig.defaultGenericClientConfig("")
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));
    multiClusterVenice = ServiceFactory.getVeniceMultiClusterWrapper("",
        2, 1, 3, 1, 3, true, false, true, Optional.of(testProperties),
        Optional.of(new VeniceProperties(testProperties)), false, Optional.empty(), Optional.empty());
    clusterNames = multiClusterVenice.getClusterNames();
    Collection<VeniceControllerWrapper> childControllers = multiClusterVenice.getControllers().values();
    // The parent controller is required for participant store setup and writing StoreAttributes and TargetVersionStates
    // for the metadata system store.
    parentController = ServiceFactory.getVeniceParentController(clusterNames, zkServer.getAddress(),
        multiClusterVenice.getKafkaBrokerWrapper(), childControllers.toArray(new VeniceControllerWrapper[0]),
        multiClusterVenice.getClusterToD2(), false, 3, new VeniceProperties(testProperties), Optional.empty());
    for (String cluster : clusterNames) {
      try (ControllerClient parentControllerClient = new ControllerClient(cluster, parentController.getControllerUrl())) {
        // Set up and configure the Zk shared store for METADATA_STORE in both clusters.
        String zkSharedStoreName = VeniceSystemStoreUtils.getSharedZkNameForMetadataStore(cluster);
        assertFalse(parentControllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedStoreName, "test").isError(),
            "Failed to create the new zk shared store");
        VersionCreationResponse response = parentControllerClient.newZkSharedStoreVersion(zkSharedStoreName);
        assertFalse(response.isError(), "Failed to create new zk shared store version");
        assertEquals(response.getVersion(), METADATA_STORE_VERSION);
        // Verify the participant store is up and running.
        String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(cluster);
        TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(participantStoreName, 1),
            parentControllerClient, 1, TimeUnit.MINUTES, Optional.empty());
      }
    }
  }

  @AfterClass
  public void cleanup() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    parentController.close();
    multiClusterVenice.close();
    zkServer.close();
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testMultiClusterDaVinci() throws Exception {
    assertTrue(clusterNames.length > 1, "Insufficient clusters for this test to be meaningful");
    int initialKeyCount = 10;
    List<String> stores = new ArrayList<>();
    // Create a new store in each cluster and setup their corresponding METADATA_STORE.
    for (int index = 0; index < clusterNames.length; index++) {
      final int value = index;
      String cluster = clusterNames[index];
      // Create the venice stores and materialize the corresponding metadata system store for each store.
      try (ControllerClient parentControllerClient = new ControllerClient(cluster, parentController.getControllerUrl())) {
        String storeName = TestUtils.getUniqueString("test-store");
        stores.add(storeName);
        assertFalse(parentControllerClient.createNewStore(storeName, "venice-test", INT_KEY_SCHEMA, INT_VALUE_SCHEMA).isError());
        VersionCreationResponse response = TestUtils.createVersionWithBatchData(parentControllerClient, storeName, INT_KEY_SCHEMA,
            INT_VALUE_SCHEMA, IntStream.range(0, initialKeyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, value)));
        assertFalse(parentControllerClient.materializeMetadataStoreVersion(storeName, METADATA_STORE_VERSION).isError(),
            "Failed to materialize metadata store version");
        // Verify the data can be ingested by classic Venice before proceeding.
        TestUtils.waitForNonDeterministicPushCompletion(response.getKafkaTopic(), parentControllerClient, 30,
            TimeUnit.SECONDS, Optional.empty());
        String metadataStoreTopicName =
            Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(storeName), METADATA_STORE_VERSION);
        TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopicName, parentControllerClient, 30,
            TimeUnit.SECONDS, Optional.empty());
        multiClusterVenice.getClusters().get(cluster).refreshAllRouterMetaData();
      }
    }
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .build();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setMemoryLimit(1024 * 1024 * 1024);
    D2Client daVinciD2 = D2TestUtils.getAndStartD2Client(multiClusterVenice.getZkServerWrapper().getAddress());

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(daVinciD2, new MetricsRepository(), backendConfig)) {
      List<DaVinciClient<Integer, Object>> clients = new ArrayList<>();
      for (int i = 0; i < stores.size(); i++) {
        String store = stores.get(i);
        DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(store, daVinciConfig);
        client.subscribeAll().get();
        for (int k = 0; k < initialKeyCount; k++) {
          assertEquals(client.get(k).get(), i);
        }
        clients.add(client);
      }
      // Verify new push works
      final int newValue = 1000;
      try (ControllerClient parentControllerClient = new ControllerClient(clusterNames[0], parentController.getControllerUrl())) {
        TestUtils.createVersionWithBatchData(parentControllerClient, stores.get(0), INT_KEY_SCHEMA,
            INT_VALUE_SCHEMA, IntStream.range(0, initialKeyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, newValue)));
      }
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        for (int k = 0; k < initialKeyCount; k++) {
          assertEquals(clients.get(0).get(k).get(), newValue);
        }
      });

      // Migrate one of the stores and perform a new push to verify store migration is also transparent for DaVinci
      final int migrateStoreIndex = stores.size() - 1;
      final String migratedStoreName = stores.get(migrateStoreIndex);
      final String srcCluster = clusterNames[migrateStoreIndex];
      final String destCluster = clusterNames[0];
      migrateStore(migratedStoreName, srcCluster, destCluster);
      final int newMigratedStoreValue = 999;
      try (ControllerClient parentControllerClient = new ControllerClient(destCluster, parentController.getControllerUrl())) {
        TestUtils.createVersionWithBatchData(parentControllerClient, migratedStoreName, INT_KEY_SCHEMA,
            INT_VALUE_SCHEMA, IntStream.range(0, initialKeyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, newMigratedStoreValue)));
      }
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        for (int k = 0; k < initialKeyCount; k++) {
          assertEquals(clients.get(migrateStoreIndex).get(k).get(), newMigratedStoreValue);
        }
      });
    } finally {
      D2ClientUtils.shutdownClient(daVinciD2);
    }
  }

  private void migrateStore(String storeName, String srcCluster, String destCluster) throws Exception {
    String[] startMigrationArgs = {"--migrate-store",
        "--url", parentController.getControllerUrl(),
        "--store", storeName,
        "--cluster-src", srcCluster,
        "--cluster-dest", destCluster};
    AdminTool.main(startMigrationArgs);
    String[] completeMigration = {"--complete-migration",
        "--url", parentController.getControllerUrl(),
        "--store", storeName,
        "--cluster-src", srcCluster,
        "--cluster-dest", destCluster,
        "--fabric", FABRIC};
    try (ControllerClient parentControllerClient = new ControllerClient(srcCluster, parentController.getControllerUrl())) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        AdminTool.main(completeMigration);
        assertEquals(parentControllerClient.discoverCluster(storeName).getCluster(), destCluster);
      });
    }
    /**
     * Add a pause between COMPLETE_MIGRATION and END_MIGRATION commands to make sure thin-client has detected the migration
     * and re-direct to the dest cluster.
     */
    Utils.sleep(10 * Time.MS_PER_SECOND);
    String[] endMigration = {"--end-migration",
        "--url", parentController.getControllerUrl(),
        "--store", storeName,
        "--cluster-src", srcCluster,
        "--cluster-dest", destCluster};
    AdminTool.main(endMigration);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testDaVinciVersionSwap() throws Exception {
    int keyCount = 10;
    String cluster = clusterNames[0];
    String storeName = TestUtils.getUniqueString("test-version-swap");
    Schema schema = Schema.parse(RECORD_VALUE_SCHEMA);
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("field1", 1);
    try (ControllerClient parentControllerClient = new ControllerClient(cluster, parentController.getControllerUrl())) {
      // Create venice store and materialize the corresponding metadata system store
      assertFalse(parentControllerClient.createNewStore(storeName, "venice-test", INT_KEY_SCHEMA, RECORD_VALUE_SCHEMA).isError());
      VersionCreationResponse response = TestUtils.createVersionWithBatchData(parentControllerClient, storeName, INT_KEY_SCHEMA,
          RECORD_VALUE_SCHEMA, IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, record1)), 1);
      assertFalse(parentControllerClient.materializeMetadataStoreVersion(storeName, METADATA_STORE_VERSION).isError(),
          "Failed to materialize metadata store version");
      // Verify the data can be ingested by classic Venice before proceeding.
      TestUtils.waitForNonDeterministicPushCompletion(response.getKafkaTopic(), parentControllerClient, 30,
          TimeUnit.SECONDS, Optional.empty());
      String metadataStoreTopicName =
          Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(storeName), METADATA_STORE_VERSION);
      TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopicName, parentControllerClient, 30,
          TimeUnit.SECONDS, Optional.empty());
      multiClusterVenice.getClusters().get(cluster).refreshAllRouterMetaData();

      VeniceProperties backendConfig = new PropertyBuilder()
          .put(DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
          .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
          .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
          .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
          .build();
      D2Client daVinciD2 = D2TestUtils.getAndStartD2Client(multiClusterVenice.getZkServerWrapper().getAddress());

      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(daVinciD2, new MetricsRepository(), backendConfig)) {
        DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());

        client.subscribeAll().get();
        for (int k = 0; k < keyCount; k++) {
          GenericData.Record value = (GenericData.Record) client.get(k).get();
          assertEquals(value.get("field1"), 1);
        }

        // Add a new value schema and push batch data
        assertFalse(parentControllerClient.addValueSchema(storeName, NEW_RECORD_VALUE_SCHEMA).isError());
        schema = Schema.parse(NEW_RECORD_VALUE_SCHEMA);
        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("field1", 2);
        record2.put("field2", 2);
        TestUtils.createVersionWithBatchData(parentControllerClient, storeName, INT_KEY_SCHEMA,
            NEW_RECORD_VALUE_SCHEMA, IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, record2)), 2);

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int k = 0; k < keyCount; k++) {
            GenericData.Record value = (GenericData.Record) client.get(k).get();
            assertEquals(value.get("field1"), 2);
            assertEquals(value.get("field2"), 2);
          }
        });
      }
    }
  }
}
