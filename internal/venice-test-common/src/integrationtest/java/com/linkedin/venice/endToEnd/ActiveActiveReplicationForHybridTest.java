package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithKeyPrefix;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.createAndVerifyStoreInAllRegions;
import static com.linkedin.venice.utils.TestUtils.updateStoreToHybrid;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskBackdoor;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.http.HttpStatus;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * TODO: Update the corresponding test cases and comments after the related Active/Active replication implementation
 *       is done.
 */
public class ActiveActiveReplicationForHybridTest {
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_MINUTE;
  private static final int PUSH_TIMEOUT = TEST_TIMEOUT / 2;

  protected static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  protected static final int NUMBER_OF_CLUSTERS = 1;
  static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  // ["venice-cluster0", "venice-cluster1", ...];

  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private D2Client d2ClientForDC0Region;
  private Properties serverProperties;
  private ControllerClient parentControllerClient;
  private ControllerClient dc0Client;
  private ControllerClient dc1Client;
  private ControllerClient dc2Client;
  private List<ControllerClient> dcControllerClientList;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 1 second;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    serverProperties.put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, true);
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "2");

    Properties controllerProps = new Properties();
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
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    // Set up a d2 client for DC0 region
    d2ClientForDC0Region = new D2ClientBuilder().setZkHosts(childDatacenters.get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2ClientForDC0Region);

    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    dc2Client = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString());
    dcControllerClientList = Arrays.asList(dc0Client, dc1Client, dc2Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (d2ClientForDC0Region != null) {
      D2ClientUtils.shutdownClient(d2ClientForDC0Region);
    }
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(dc0Client);
    Utils.closeQuietlyWithErrorLogged(dc1Client);
    Utils.closeQuietlyWithErrorLogged(dc2Client);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEnableActiveActiveReplicationForCluster() {
    String storeName1 = Utils.getUniqueString("test-batch-store");
    String storeName2 = Utils.getUniqueString("test-hybrid-agg-store");
    String storeName3 = Utils.getUniqueString("test-hybrid-non-agg-store");
    String storeName4 = Utils.getUniqueString("test-incremental-push-store");
    try {
      createAndVerifyStoreInAllRegions(storeName1, parentControllerClient, dcControllerClientList);
      createAndVerifyStoreInAllRegions(storeName2, parentControllerClient, dcControllerClientList);
      createAndVerifyStoreInAllRegions(storeName3, parentControllerClient, dcControllerClientList);
      createAndVerifyStoreInAllRegions(storeName4, parentControllerClient, dcControllerClientList);

      assertCommand(
          parentControllerClient.updateStore(
              storeName2,
              new UpdateStoreQueryParams().setHybridRewindSeconds(10)
                  .setHybridOffsetLagThreshold(2)
                  .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE)));

      assertCommand(
          parentControllerClient.updateStore(
              storeName3,
              new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(2)));

      assertCommand(
          parentControllerClient.updateStore(storeName4, new UpdateStoreQueryParams().setIncrementalPushEnabled(true)));

      // Test batch
      assertCommand(
          parentControllerClient.configureActiveActiveReplicationForCluster(
              true,
              VeniceUserStoreType.BATCH_ONLY.toString(),
              Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName1, false, false, true);
      verifyDCConfigAARepl(dc0Client, storeName1, false, false, true);
      verifyDCConfigAARepl(dc1Client, storeName1, false, false, true);
      verifyDCConfigAARepl(dc2Client, storeName1, false, false, true);
      assertCommand(
          assertCommand(
              parentControllerClient.configureActiveActiveReplicationForCluster(
                  false,
                  VeniceUserStoreType.BATCH_ONLY.toString(),
                  Optional.of("dc-parent-0.parent,dc-0"))));
      verifyDCConfigAARepl(parentControllerClient, storeName1, false, true, false);
      verifyDCConfigAARepl(dc0Client, storeName1, false, true, false);
      verifyDCConfigAARepl(dc1Client, storeName1, false, true, true);
      verifyDCConfigAARepl(dc2Client, storeName1, false, true, true);

      // Test hybrid - agg vs non-agg
      assertCommand(
          parentControllerClient.configureActiveActiveReplicationForCluster(
              true,
              VeniceUserStoreType.HYBRID_ONLY.toString(),
              Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName2, true, false, false);
      verifyDCConfigAARepl(dc0Client, storeName2, true, false, false);
      verifyDCConfigAARepl(dc1Client, storeName2, true, false, false);
      verifyDCConfigAARepl(dc2Client, storeName2, true, false, false);
      verifyDCConfigAARepl(parentControllerClient, storeName3, true, false, true);
      verifyDCConfigAARepl(dc0Client, storeName3, true, false, true);
      verifyDCConfigAARepl(dc1Client, storeName3, true, false, true);
      verifyDCConfigAARepl(dc2Client, storeName3, true, false, true);
      assertCommand(
          parentControllerClient.configureActiveActiveReplicationForCluster(
              false,
              VeniceUserStoreType.HYBRID_ONLY.toString(),
              Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName3, true, true, false);
      verifyDCConfigAARepl(dc0Client, storeName3, true, true, false);
      verifyDCConfigAARepl(dc1Client, storeName3, true, true, false);
      verifyDCConfigAARepl(dc2Client, storeName3, true, true, false);

      // Test incremental
      assertCommand(
          parentControllerClient.configureActiveActiveReplicationForCluster(
              true,
              VeniceUserStoreType.INCREMENTAL_PUSH.toString(),
              Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName4, false, false, true);
      verifyDCConfigAARepl(dc0Client, storeName4, false, false, true);
      verifyDCConfigAARepl(dc1Client, storeName4, false, false, true);
      verifyDCConfigAARepl(dc2Client, storeName4, false, false, true);
    } finally {
      deleteStores(storeName1, storeName2, storeName3, storeName4);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEnableNRisRequiredBeforeEnablingAA() {
    String storeName = Utils.getUniqueString("test-store");
    String anotherStoreName = Utils.getUniqueString("test-store");
    try {
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));

      // Expect the request to fail since AA cannot be enabled without enabling NR
      try {
        updateStoreToHybrid(
            storeName,
            parentControllerClient,
            Optional.of(false),
            Optional.of(true),
            Optional.of(false));
        fail("The update store command should not have succeeded since AA cannot be enabled without enabling NR.");
      } catch (AssertionError e) {
        assertTrue(e.getMessage().contains("Http Status " + HttpStatus.SC_BAD_REQUEST)); // Must contain the correct
                                                                                         // HTTP status code
      }

      // Expect the request to succeed
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(false));

      // Create a new store
      assertCommand(parentControllerClient.createNewStore(anotherStoreName, "owner", STRING_SCHEMA, STRING_SCHEMA));

      // Enable NR
      updateStoreToHybrid(
          anotherStoreName,
          parentControllerClient,
          Optional.of(true),
          Optional.of(false),
          Optional.of(false));

      // Enable AA after NR is enabled (expect to succeed)
      updateStoreToHybrid(
          anotherStoreName,
          parentControllerClient,
          Optional.empty(),
          Optional.of(true),
          Optional.of(false));

      // Disable NR and enable AA (expect to fail)
      try {
        updateStoreToHybrid(
            anotherStoreName,
            parentControllerClient,
            Optional.of(false),
            Optional.of(true),
            Optional.of(false));
        fail("The update store command should not have succeeded since AA cannot be enabled without enabling NR.");
      } catch (AssertionError e) {
        assertTrue(e.getMessage().contains("Http Status " + HttpStatus.SC_BAD_REQUEST)); // Must contain the correct
                                                                                         // HTTP status code
      }
    } finally {
      deleteStores(storeName, anotherStoreName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAAReplicationCanConsumeFromAllRegions(boolean isChunkingEnabled, boolean useTransientRecordCache)
      throws InterruptedException, ExecutionException {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    try {
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));
      updateStoreToHybrid(
          storeName,
          parentControllerClient,
          Optional.of(true),
          Optional.of(true),
          Optional.of(isChunkingEnabled));

      // Empty push to create a version
      ControllerResponse controllerResponse = assertCommand(
          parentControllerClient
              .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, PUSH_TIMEOUT));
      assertTrue(controllerResponse instanceof JobStatusQueryResponse);
      JobStatusQueryResponse jobStatusQueryResponse = (JobStatusQueryResponse) controllerResponse;
      int versionNumber = jobStatusQueryResponse.getVersion();
      // Wait for push to complete in all regions
      waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        for (ControllerClient controllerClient: dcControllerClientList) {
          StoreResponse storeResponse = assertCommand(controllerClient.getStore(storeName));
          assertEquals(storeResponse.getStore().getCurrentVersion(), versionNumber);
        }
      });

      // disable the purging of transientRecord buffer using reflection.
      if (useTransientRecordCache) {
        String topicName = Version.composeKafkaTopic(storeName, versionNumber);
        for (VeniceMultiClusterWrapper veniceRegion: multiRegionMultiClusterWrapper.getChildRegions()) {
          VeniceClusterWrapper veniceCluster = veniceRegion.getClusters().get(clusterName);
          for (VeniceServerWrapper veniceServerWrapper: veniceCluster.getVeniceServers()) {
            StoreIngestionTaskBackdoor.setPurgeTransientRecordBuffer(veniceServerWrapper, topicName, false);
          }
        }
      }

      Map<VeniceMultiClusterWrapper, VeniceSystemProducer> childDatacenterToSystemProducer =
          new HashMap<>(NUMBER_OF_CHILD_DATACENTERS);
      int streamingRecordCount = 10;
      try {
        for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
          // Send messages to RT in the corresponding region
          String keyPrefix = "dc-" + dataCenterIndex + "_key_";
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(dataCenterIndex);

          Map<String, String> samzaConfig = new HashMap<>();
          String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
          samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
          samzaConfig.put(configPrefix + VENICE_STORE, storeName);
          samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
          samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, childDataCenter.getZkServerWrapper().getAddress());
          samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
          samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress());
          samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
          samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
          samzaConfig.put(SSL_ENABLED, "false");
          VeniceSystemFactory factory = new VeniceSystemFactory();
          VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null);
          veniceProducer.start();
          childDatacenterToSystemProducer.put(childDataCenter, veniceProducer);

          for (int i = 0; i < streamingRecordCount; i++) {
            sendStreamingRecordWithKeyPrefix(veniceProducer, storeName, keyPrefix, i);
          }
        }

        // Server in dc-0 data center should serve real-time data from all different regions
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
        String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
        try (AvroGenericStoreClient<String, Object> client = ClientFactory
            .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
          waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              // Verify the data sent by Samza producer from different regions
              String keyPrefix = "dc-" + dataCenterIndex + "_key_";
              for (int i = 0; i < streamingRecordCount; i++) {
                String expectedValue = "stream_" + i;
                Object valueObject = client.get(keyPrefix + i).get();
                if (valueObject == null) {
                  fail(
                      "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex + " for key: "
                          + keyPrefix + i);
                } else {
                  assertEquals(
                      valueObject.toString(),
                      expectedValue,
                      "Servers in dc-0 contain corrupted data sent from region dc-" + dataCenterIndex);
                }
              }
            }
          });

          // Send DELETE from all child datacenter for existing and new records
          for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
            String keyPrefix = "dc-" + dataCenterIndex + "_key_";
            sendStreamingDeleteRecord(
                childDatacenterToSystemProducer.get(childDatacenters.get(dataCenterIndex)),
                storeName,
                keyPrefix + (streamingRecordCount - 1));
            sendStreamingDeleteRecord(
                childDatacenterToSystemProducer.get(childDatacenters.get(dataCenterIndex)),
                storeName,
                keyPrefix + streamingRecordCount);
          }

          // Verify both DELETEs can be processed
          waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              // Verify the data sent by Samza producer from different regions
              String keyPrefix = "dc-" + dataCenterIndex + "_key_";
              assertNull(
                  client.get(keyPrefix + (streamingRecordCount - 1)).get(),
                  "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
              assertNull(
                  client.get(keyPrefix + streamingRecordCount).get(),
                  "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
            }
          });

          // Send PUT from all child datacenter for new records
          for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
            String keyPrefix = "dc-" + dataCenterIndex + "_key_";
            sendStreamingRecordWithKeyPrefix(
                childDatacenterToSystemProducer.get(childDatacenters.get(dataCenterIndex)),
                storeName,
                keyPrefix,
                streamingRecordCount);
          }

          waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              // Verify the data sent by Samza producer from different regions
              String keyPrefix = "dc-" + dataCenterIndex + "_key_";
              assertNull(
                  client.get(keyPrefix + (streamingRecordCount - 1)).get(),
                  "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
              String expectedValue = "stream_" + streamingRecordCount;
              Object valueObject = client.get(keyPrefix + streamingRecordCount).get();
              if (valueObject == null) {
                fail("Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
              } else {
                assertEquals(
                    valueObject.toString(),
                    expectedValue,
                    "Servers in dc-0 contain corrupted data sent from region dc-" + dataCenterIndex);
              }
            }
          });
        }
      } finally {
        for (VeniceSystemProducer veniceProducer: childDatacenterToSystemProducer.values()) {
          Utils.closeQuietlyWithErrorLogged(veniceProducer);
        }
      }

      // Verify that DaVinci client can successfully bootstrap all partitions from AA enabled stores
      String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
      VeniceProperties backendConfig =
          new PropertyBuilder().put(DATA_BASE_PATH, baseDataPath).put(PERSISTENCE_TYPE, ROCKS_DB).build();

      MetricsRepository metricsRepository = new MetricsRepository();
      try (
          CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
              d2ClientForDC0Region,
              VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
              metricsRepository,
              backendConfig);
          DaVinciClient<String, Object> daVinciClient =
              factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig())) {
        daVinciClient.subscribeAll().get();
        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
            // Verify the data sent by Samza producer from different regions
            String keyPrefix = "dc-" + dataCenterIndex + "_key_";
            assertNull(
                daVinciClient.get(keyPrefix + (streamingRecordCount - 1)).get(),
                "DaVinci clients in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
            String expectedValue = "stream_" + streamingRecordCount;
            Object valueObject = daVinciClient.get(keyPrefix + streamingRecordCount).get();
            if (valueObject == null) {
              fail("DaVinci clients in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
            } else {
              assertEquals(
                  valueObject.toString(),
                  expectedValue,
                  "DaVinci clients in dc-0 contain corrupted data sent from region dc-" + dataCenterIndex);
            }
          }
        });
      }
    } finally {
      deleteStores(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetStoreReplicationMetadataSchema() {
    String storeName = Utils.getUniqueString("test-store");
    try {
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(false));

      // Empty push to create a version
      assertCommand(parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L));
      MultiSchemaResponse schemaResponse =
          assertCommand(parentControllerClient.getAllReplicationMetadataSchemas(storeName));
      String expectedSchema =
          "{\"type\":\"record\",\"name\":\"string_MetadataRecord\",\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"timestamp\",\"type\":[\"long\"],\"doc\":\"timestamp when the full record was last updated\",\"default\":0},{\"name\":\"replication_checkpoint_vector\",\"type\":{\"type\":\"array\",\"items\":\"long\"},\"doc\":\"high watermark remote checkpoints which touched this record\",\"default\":[]}]}";
      assertEquals(schemaResponse.getSchemas()[0].getSchemaStr(), expectedSchema);
      assertEquals(schemaResponse.getSchemas()[0].getRmdValueSchemaId(), 1);
      assertEquals(schemaResponse.getSchemas()[0].getId(), 1);
    } finally {
      deleteStores(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAAReplicationCanResolveConflicts(boolean useLogicalTimestamp, boolean chunkingEnabled) {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    try {
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));
      updateStoreToHybrid(
          storeName,
          parentControllerClient,
          Optional.of(true),
          Optional.of(true),
          Optional.of(chunkingEnabled));

      // Empty push to create a version
      assertCommand(
          parentControllerClient
              .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, PUSH_TIMEOUT));

      // Verify that version 1 is already created in dc-0 region
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = assertCommand(dc0Client.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
      });

      /**
       * First test:
       * Servers can resolve conflicts within the same regions; there could be multiple Samza processors sending messages
       * with the same key in the same region, so there could be conflicts within the same region.
       */
      // Build a list of mock time
      List<Long> mockTimestampInMs = new LinkedList<>();
      long baselineTimestampInMs = System.currentTimeMillis();
      if (!useLogicalTimestamp) {
        // Timestamp for segment start time bookkeeping
        mockTimestampInMs.add(baselineTimestampInMs);
        // Timestamp for START_OF_SEGMENT message
        mockTimestampInMs.add(baselineTimestampInMs);
      }
      // Timestamp for Key1
      mockTimestampInMs.add(baselineTimestampInMs);
      // Timestamp for Key1 with a different value and a bigger offset; since it has an older timestamp, its value will
      // not override the previous value even though it will arrive at the Kafka topic later
      mockTimestampInMs.add(baselineTimestampInMs - 10);
      // Timestamp for Key2 with the highest offset, which will be used to verify that all messages in RT have been
      // processed
      mockTimestampInMs.add(baselineTimestampInMs);
      Time mockTime = new MockCircularTime(mockTimestampInMs);
      String key1 = "key1";
      String value1 = "value1";
      String key2 = "key2";
      String value2 = "value2";

      // Build the SystemProducer with the mock time
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
      try (VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
          childDataCenter.getZkServerWrapper().getAddress(),
          childDataCenter.getZkServerWrapper().getAddress(),
          D2_SERVICE_NAME,
          storeName,
          Version.PushType.STREAM,
          Utils.getUniqueString("venice-push-id"),
          "dc-0",
          true,
          null,
          Optional.empty(),
          Optional.empty(),
          mockTime)) {
        producerInDC0.start();

        // Send <Key1, Value1>
        OutgoingMessageEnvelope envelope1 = new OutgoingMessageEnvelope(
            new SystemStream("venice", storeName),
            key1,
            useLogicalTimestamp ? new VeniceObjectWithTimestamp(value1, mockTime.getMilliseconds()) : value1);
        producerInDC0.send(storeName, envelope1);

        // Send <Key1, Value2>, which will be ignored by Servers if DCR is properly supported
        OutgoingMessageEnvelope envelope2 = new OutgoingMessageEnvelope(
            new SystemStream("venice", storeName),
            key1,
            useLogicalTimestamp ? new VeniceObjectWithTimestamp(value2, mockTime.getMilliseconds()) : value2);
        producerInDC0.send(storeName, envelope2);

        // Send <Key1, Value1> with same timestamp to trigger direct object comparison
        producerInDC0.send(storeName, envelope1);

        // Send <Key2, Value1>, which is used to verify that servers have consumed and processed till the end of all
        // real-time messages
        OutgoingMessageEnvelope envelope3 = new OutgoingMessageEnvelope(
            new SystemStream("venice", storeName),
            key2,
            useLogicalTimestamp ? new VeniceObjectWithTimestamp(value1, mockTime.getMilliseconds()) : value1);
        producerInDC0.send(storeName, envelope3);
      }

      // Verify data in dc-0
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory
          .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {

        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          // Check <Key2, Value1> has been consumed
          Object valueObject = client.get(key2).get();
          assertNotNull(valueObject);
          assertEquals(valueObject.toString(), value1);
          // Check <Key1, Value2> was dropped, so that Key1 will have value equal to Value1
          Object valueObject1 = client.get(key1).get();
          assertNotNull(valueObject1);
          assertEquals(valueObject1.toString(), value1, "DCR is not working properly");
        });
      }

      /**
       * Second test:
       * Servers can resolve conflicts from different regions.
       */
      // Build a list of mock time
      mockTimestampInMs = new LinkedList<>();
      if (!useLogicalTimestamp) {
        // Timestamp for segment start time bookkeeping
        mockTimestampInMs.add(baselineTimestampInMs);
        // Timestamp for START_OF_SEGMENT message
        mockTimestampInMs.add(baselineTimestampInMs);
      }
      // Timestamp for Key1 with a different value from dc-1 region; it will be consumed later than all messages in
      // dc-0,
      // but since it has an older timestamp, its value will not override the previous value even though it will arrive
      // at dc-0 servers later
      mockTimestampInMs.add(baselineTimestampInMs - 5);
      // Timestamp for Key3 with the highest offset in dc-1 RT, which will be used to verify that all messages in dc-1
      // RT have been processed
      mockTimestampInMs.add(baselineTimestampInMs);
      mockTime = new MockCircularTime(mockTimestampInMs);
      String key3 = "key3";
      String value3 = "value3";

      // Build the SystemProducer with the mock time
      VeniceMultiClusterWrapper childDataCenter1 = childDatacenters.get(1);
      try (VeniceSystemProducer producerInDC1 = new VeniceSystemProducer(
          childDataCenter.getZkServerWrapper().getAddress(),
          childDataCenter1.getZkServerWrapper().getAddress(),
          D2_SERVICE_NAME,
          storeName,
          Version.PushType.STREAM,
          Utils.getUniqueString("venice-push-id"),
          "dc-1",
          true,
          null,
          Optional.empty(),
          Optional.empty(),
          mockTime)) {
        producerInDC1.start();

        // Send <Key1, Value3>, which will be ignored if DCR is implemented properly
        OutgoingMessageEnvelope envelope4 = new OutgoingMessageEnvelope(
            new SystemStream("venice", storeName),
            key1,
            useLogicalTimestamp ? new VeniceObjectWithTimestamp(value3, mockTime.getMilliseconds()) : value3);
        producerInDC1.send(storeName, envelope4);

        // Send <Key3, Value1>, which is used to verify that servers have consumed and processed till the end of all
        // real-time messages from dc-1
        OutgoingMessageEnvelope envelope5 = new OutgoingMessageEnvelope(
            new SystemStream("venice", storeName),
            key3,
            useLogicalTimestamp ? new VeniceObjectWithTimestamp(value1, mockTime.getMilliseconds()) : value1);
        producerInDC1.send(storeName, envelope5);
      }

      // Verify data in dc-0
      try (AvroGenericStoreClient<String, Object> client = ClientFactory
          .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {

        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          // Check <Key3, Value1> has been consumed
          Object valueObject = client.get(key3).get();
          assertNotNull(valueObject);
          assertEquals(valueObject.toString(), value1);
          // Check <Key1, Value3> was dropped, so that Key1 will have value equal to Value1
          Object valueObject1 = client.get(key1).get();
          assertNotNull(valueObject1);
          assertEquals(valueObject1.toString(), value1, "DCR is not working properly");
        });
      }
    } finally {
      deleteStores(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHelixReplicationFactorConfigChange() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    VeniceClusterWrapper clusterForDC0Region = childDatacenters.get(0).getClusters().get(clusterName);
    String kafkaTopic;

    try {
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA));
      updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(true));
      // Empty push to create a version
      ControllerResponse response = assertCommand(
          parentControllerClient
              .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L, PUSH_TIMEOUT));
      assertTrue(response instanceof JobStatusQueryResponse);
      JobStatusQueryResponse jobStatusQueryResponse = (JobStatusQueryResponse) response;
      kafkaTopic = Version.composeKafkaTopic(storeName, jobStatusQueryResponse.getVersion());
      // Verify that version 1 is already created in dc-0 region, and there are less than 3 ready-to-serve instances
      OnlineInstanceFinder onlineInstanceFinder =
          clusterForDC0Region.getRandomVeniceRouter().getRoutingDataRepository();
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = assertCommand(dc0Client.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
        List<Instance> instances = onlineInstanceFinder.getReadyToServeInstances(kafkaTopic, 0);
        assertTrue(instances.size() < 3);
      });

      VeniceServerWrapper server = null;
      HelixAdmin helixAdminForDC0Region = null;
      try {
        // Add the third server in dc-0 region. Update Helix RF config from 2 to 3
        server = clusterForDC0Region.addVeniceServer(new Properties(), serverProperties);
        helixAdminForDC0Region = new ZKHelixAdmin(clusterForDC0Region.getZk().getAddress());
        IdealState idealState = helixAdminForDC0Region.getResourceIdealState(clusterName, kafkaTopic);
        idealState.setReplicas("3");
        helixAdminForDC0Region.setResourceIdealState(clusterName, kafkaTopic, idealState);
        // Expect to have 3 ready-to-serve instances
        OnlineInstanceFinder onlineInstanceFinder2 =
            clusterForDC0Region.getRandomVeniceRouter().getRoutingDataRepository();
        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          List<Instance> instances = onlineInstanceFinder2.getReadyToServeInstances(kafkaTopic, 0);
          assertEquals(instances.size(), 3);
        });
      } finally {
        if (server != null) {
          clusterForDC0Region.removeVeniceServer(server.getPort());
        }
        if (helixAdminForDC0Region != null) {
          helixAdminForDC0Region.close();
        }
      }
    } finally {
      deleteStores(storeName);
    }
  }

  public static void verifyDCConfigAARepl(
      ControllerClient controllerClient,
      String storeName,
      boolean isHybrid,
      boolean currentStatus,
      boolean expectedStatus) {
    waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = assertCommand(controllerClient.getStore(storeName));
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(
          storeInfo.isActiveActiveReplicationEnabled(),
          expectedStatus,
          "The active active replication config does not match.");
      if (isHybrid && (currentStatus != expectedStatus)) {
        HybridStoreConfig hybridStoreConfig = storeInfo.getHybridStoreConfig();
        assertNotNull(hybridStoreConfig);
        DataReplicationPolicy policy = hybridStoreConfig.getDataReplicationPolicy();
        assertEquals(
            policy,
            DataReplicationPolicy.NON_AGGREGATE,
            "The active active replication policy does not match.");
      }
    });
  }

  private void deleteStores(String... storeNames) {
    CompletableFuture.runAsync(() -> {
      try {
        for (String storeName: storeNames) {
          parentControllerClient.disableAndDeleteStore(storeName);
        }
      } catch (Exception e) {
        // ignore... this is just best-effort.
      }
    });
  }
}
