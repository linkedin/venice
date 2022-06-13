package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.http.HttpStatus;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


/**
 * TODO: Update the corresponding test cases and comments after the related Active/Active replication implementation
 *       is done.
 */
public class ActiveActiveReplicationForHybridTest {
  private static final int TEST_TIMEOUT = 120_000; // ms

  protected static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  protected static final int NUMBER_OF_CLUSTERS = 1;
  protected static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  // ["venice-cluster0", "venice-cluster1", ...];

  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  private D2Client d2ClientForDC0Region;
  private Properties serverProperties;

  public Map<String, Object> getExtraServerProperties() {
    return Collections.emptyMap();
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 1 second;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, true);
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    serverProperties.put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, true);
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "2");
    serverProperties.putAll(getExtraServerProperties());

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);

    controllerProps.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, true);
    controllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME + ",dc-0");
    int parentKafkaPort = Utils.getFreePort();
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME, "localhost:" + parentKafkaPort);

    multiColoMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
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
            false,
            MirrorMakerWrapper.DEFAULT_TOPIC_ALLOWLIST,
            false,
            Optional.of(parentKafkaPort));
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    // Set up a d2 client for DC0 region
    d2ClientForDC0Region = new D2ClientBuilder()
        .setZkHosts(childDatacenters.get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2ClientForDC0Region);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (d2ClientForDC0Region != null) {
      D2ClientUtils.shutdownClient(d2ClientForDC0Region);
    }
    Utils.closeQuietlyWithErrorLogged(multiColoMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEnableActiveActiveReplicationForCluster() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName1 = Utils.getUniqueString("test-batch-store");
    String storeName2 = Utils.getUniqueString("test-hybrid-agg-store");
    String storeName3 = Utils.getUniqueString("test-hybrid-non-agg-store");
    String storeName4 = Utils.getUniqueString("test-incremental-push-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
        ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client, dc2Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName1, parentControllerClient, dcControllerClientList);
      TestUtils.createAndVerifyStoreInAllRegions(storeName2, parentControllerClient, dcControllerClientList);
      TestUtils.createAndVerifyStoreInAllRegions(storeName3, parentControllerClient, dcControllerClientList);
      TestUtils.createAndVerifyStoreInAllRegions(storeName4, parentControllerClient, dcControllerClientList);

      TestUtils.assertCommand(parentControllerClient.updateStore(storeName1, new UpdateStoreQueryParams()
          .setLeaderFollowerModel(true)));

      TestUtils.assertCommand(parentControllerClient.updateStore(storeName2, new UpdateStoreQueryParams()
          .setLeaderFollowerModel(true)
          .setHybridRewindSeconds(10)
          .setHybridOffsetLagThreshold(2)
          .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE)));

      TestUtils.assertCommand(parentControllerClient.updateStore(storeName3, new UpdateStoreQueryParams()
          .setLeaderFollowerModel(true)
          .setHybridRewindSeconds(10)
          .setHybridOffsetLagThreshold(2)));

      TestUtils.assertCommand(parentControllerClient.updateStore(storeName4, new UpdateStoreQueryParams()
          .setIncrementalPushEnabled(true)
          .setLeaderFollowerModel(true)));

      // Test batch
      TestUtils.assertCommand(parentControllerClient.configureActiveActiveReplicationForCluster(
          true, VeniceUserStoreType.BATCH_ONLY.toString(), Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName1, false, false,true);
      verifyDCConfigAARepl(dc0Client, storeName1, false, false, true);
      verifyDCConfigAARepl(dc1Client, storeName1, false, false, true);
      verifyDCConfigAARepl(dc2Client, storeName1, false,false, true);
      TestUtils.assertCommand(parentControllerClient.configureActiveActiveReplicationForCluster(
          false, VeniceUserStoreType.BATCH_ONLY.toString(), Optional.of("parent.parent,dc-0")));
      verifyDCConfigAARepl(parentControllerClient, storeName1, false, true, false);
      verifyDCConfigAARepl(dc0Client, storeName1, false, true, false);
      verifyDCConfigAARepl(dc1Client, storeName1, false, true, true);
      verifyDCConfigAARepl(dc2Client, storeName1, false, true, true);

      // Test hybrid - agg vs non-agg
      TestUtils.assertCommand(parentControllerClient.configureActiveActiveReplicationForCluster(
          true, VeniceUserStoreType.HYBRID_ONLY.toString(), Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName2, true, false, false);
      verifyDCConfigAARepl(dc0Client, storeName2, true, false, false);
      verifyDCConfigAARepl(dc1Client, storeName2, true, false,false);
      verifyDCConfigAARepl(dc2Client, storeName2, true,false, false);
      verifyDCConfigAARepl(parentControllerClient, storeName3, true, false, true);
      verifyDCConfigAARepl(dc0Client, storeName3, true, false, true);
      verifyDCConfigAARepl(dc1Client, storeName3, true, false,true);
      verifyDCConfigAARepl(dc2Client, storeName3, true,false, true);
      TestUtils.assertCommand(parentControllerClient.configureActiveActiveReplicationForCluster(
          false, VeniceUserStoreType.HYBRID_ONLY.toString(), Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName3, true, true, false);
      verifyDCConfigAARepl(dc0Client, storeName3, true, true, false);
      verifyDCConfigAARepl(dc1Client, storeName3, true, true,false);
      verifyDCConfigAARepl(dc2Client, storeName3, true,true, false);

      // Test incremental
      TestUtils.assertCommand(parentControllerClient.configureActiveActiveReplicationForCluster(
          true, VeniceUserStoreType.INCREMENTAL_PUSH.toString(), Optional.empty()));
      verifyDCConfigAARepl(parentControllerClient, storeName4, false, false, true);
      verifyDCConfigAARepl(dc0Client, storeName4, false, false, true);
      verifyDCConfigAARepl(dc1Client, storeName4, false, false,true);
      verifyDCConfigAARepl(dc2Client, storeName4, false,false, true);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEnableNRisRequiredBeforeEnablingAA() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);

      // Expect the request to fail since AA cannot be enabled without enabling NR
      ControllerResponse controllerResponse = updateStore(storeName, parentControllerClient, Optional.of(false), Optional.of(true), Optional.of(false), CompressionStrategy.NO_OP);
      Assert.assertTrue(controllerResponse.isError());
      Assert.assertTrue(controllerResponse.getError().contains("Http Status " + HttpStatus.SC_BAD_REQUEST)); // Must contain the correct HTTP status code

      // Expect the request to succeed
      TestUtils.assertCommand(
          updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(false), CompressionStrategy.NO_OP));

      // Create a new store
      String anotherStoreName = Utils.getUniqueString("test-store");
      parentControllerClient.createNewStore(anotherStoreName, "owner", STRING_SCHEMA, STRING_SCHEMA);

      // Enable NR
      TestUtils.assertCommand(
          updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(false), Optional.of(false), CompressionStrategy.NO_OP));

      // Enable AA after NR is enabled (expect to succeed)
      TestUtils.assertCommand(
          updateStore(storeName, parentControllerClient, Optional.empty(), Optional.of(true), Optional.of(false), CompressionStrategy.NO_OP));

      // Disable NR and enable AA (expect to fail)
      controllerResponse = updateStore(storeName, parentControllerClient, Optional.of(false), Optional.of(true), Optional.of(false), CompressionStrategy.NO_OP);
      Assert.assertTrue(controllerResponse.isError());
      Assert.assertTrue(controllerResponse.getError().contains("Http Status " + HttpStatus.SC_BAD_REQUEST)); // Must contain the correct HTTP status code
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Boolean-Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testAAReplicationCanConsumeFromAllRegions(boolean isChunkingEnabled, boolean useTransientRecordCache, CompressionStrategy compressionStrategy)
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    String pzkAddress = parentController.getZkAddress();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
      updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(isChunkingEnabled), compressionStrategy);

      // Empty push to create a version
      VersionCreationResponse versionCreationResponse = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);

      //disable the purging of transientRecord buffer using reflection.
      if (useTransientRecordCache) {
        for (VeniceMultiClusterWrapper veniceColo : multiColoMultiClusterWrapper.getClusters()) {
          VeniceClusterWrapper veniceCluster = veniceColo.getClusters().get(clusterName);
          // Wait for push to complete in the colo
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () ->
              Assert.assertEquals(veniceCluster.getControllerClient().getStore(storeName).getStore().getCurrentVersion(),
                  Version.parseVersionFromKafkaTopicName(versionCreationResponse.getKafkaTopic())));
          for (VeniceServerWrapper veniceServerWrapper : veniceCluster.getVeniceServers()){
            VeniceServer veniceServer = veniceServerWrapper.getVeniceServer();
            StoreIngestionTask ingestionTask = veniceServer.getKafkaStoreIngestionService().getStoreIngestionTask(versionCreationResponse.getKafkaTopic());
            Field purgeTransientRecordBufferField =
                ingestionTask.getClass().getSuperclass().getSuperclass().getDeclaredField("purgeTransientRecordBuffer");
            purgeTransientRecordBufferField.setAccessible(true);
            purgeTransientRecordBufferField.setBoolean(ingestionTask, false);
          }
        }
      }

      Map<VeniceMultiClusterWrapper, VeniceSystemProducer> childDatacenterToSystemProducer = new HashMap<>(NUMBER_OF_CHILD_DATACENTERS);
      int streamingRecordCount = 10;
      try {
        for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
          // Send messages to RT in the corresponding region
          String keyPrefix = "dc-" + dataCenterIndex + "_key_";
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(dataCenterIndex);
          String zkAddress = childDataCenter.getZkServerWrapper().getAddress();

          try (ControllerClient childControllerClient = new ControllerClient(clusterName,
              childDataCenter.getLeaderController(clusterName).getControllerUrl())) {
            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              StoreResponse storeResponse = TestUtils.assertCommand(childControllerClient.getStore(storeName));
              Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
            });
          }

          Map<String, String> samzaConfig = new HashMap<>();
          String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
          samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
          samzaConfig.put(configPrefix + VENICE_STORE, storeName);
          samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
          samzaConfig.put(D2_ZK_HOSTS_PROPERTY, childDataCenter.getZkServerWrapper().getAddress());
          samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, parentController.getKafkaZkAddress());
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
        try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              // Verify the data sent by Samza producer from different regions
              String keyPrefix = "dc-" + dataCenterIndex + "_key_";
              for (int i = 0; i < streamingRecordCount; i++) {
                String expectedValue = "stream_" + i;
                Object valueObject = client.get(keyPrefix + i).get();
                if (valueObject == null) {
                  Assert.fail("Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex + " for key: "
                      + keyPrefix + i);
                } else {
                  Assert.assertEquals(valueObject.toString(), expectedValue,
                      "Servers in dc-0 contain corrupted data sent from region dc-" + dataCenterIndex);
                }
              }
            }
          });

          // Send DELETE from all child datacenter for existing and new records
          for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
            String keyPrefix = "dc-" + dataCenterIndex + "_key_";
            sendStreamingDeleteRecord(childDatacenterToSystemProducer.get(childDatacenters.get(dataCenterIndex)),
                storeName, keyPrefix + (streamingRecordCount - 1));
            sendStreamingDeleteRecord(childDatacenterToSystemProducer.get(childDatacenters.get(dataCenterIndex)),
                storeName, keyPrefix + streamingRecordCount);
          }

          // Verify both DELETEs can be processed
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              // Verify the data sent by Samza producer from different regions
              String keyPrefix = "dc-" + dataCenterIndex + "_key_";
              Assert.assertNull(client.get(keyPrefix + (streamingRecordCount - 1)).get(),
                  "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
              Assert.assertNull(client.get(keyPrefix + streamingRecordCount).get(),
                  "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
            }
          });

          // Send PUT from all child datacenter for new records
          for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
            String keyPrefix = "dc-" + dataCenterIndex + "_key_";
            sendStreamingRecordWithKeyPrefix(childDatacenterToSystemProducer.get(childDatacenters.get(dataCenterIndex)),
                storeName, keyPrefix, streamingRecordCount);
          }

          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              // Verify the data sent by Samza producer from different regions
              String keyPrefix = "dc-" + dataCenterIndex + "_key_";
              Assert.assertNull(client.get(keyPrefix + (streamingRecordCount - 1)).get(),
                  "Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
              String expectedValue = "stream_" + streamingRecordCount;
              Object valueObject = client.get(keyPrefix + streamingRecordCount).get();
              if (valueObject == null) {
                Assert.fail("Servers in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
              } else {
                Assert.assertEquals(valueObject.toString(), expectedValue,
                    "Servers in dc-0 contain corrupted data sent from region dc-" + dataCenterIndex);
              }
            }
          });
        }
      } finally {
        for (VeniceSystemProducer veniceProducer : childDatacenterToSystemProducer.values()) {
          Utils.closeQuietlyWithErrorLogged(veniceProducer);
        }
      }

      // Verify that DaVinci client can successfully bootstrap all partitions from AA enabled stores
      String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
      VeniceProperties backendConfig = new PropertyBuilder()
          .put(DATA_BASE_PATH, baseDataPath)
          .put(PERSISTENCE_TYPE, ROCKS_DB)
          .build();

      MetricsRepository metricsRepository = new MetricsRepository();
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2ClientForDC0Region, metricsRepository, backendConfig)) {
        DaVinciClient<String, Object> daVinciClient = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
        daVinciClient.subscribeAll().get();
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
          for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
            // Verify the data sent by Samza producer from different regions
            String keyPrefix = "dc-" + dataCenterIndex + "_key_";
            Assert.assertNull(daVinciClient.get(keyPrefix + (streamingRecordCount - 1)).get(),
                "DaVinci clients in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
            String expectedValue = "stream_" + streamingRecordCount;
            Object valueObject = daVinciClient.get(keyPrefix + streamingRecordCount).get();
            if (valueObject == null) {
              Assert.fail("DaVinci clients in dc-0 haven't consumed real-time data from region dc-" + dataCenterIndex);
            } else {
              Assert.assertEquals(valueObject.toString(), expectedValue, "DaVinci clients in dc-0 contain corrupted data sent from region dc-" + dataCenterIndex);
            }
          }
        });
        daVinciClient.close();
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetStoreReplicationMetadataSchema() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
      updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(false), CompressionStrategy.NO_OP);

      // Empty push to create a version
      parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      MultiSchemaResponse schemaResponse = TestUtils.assertCommand(
          parentControllerClient.getAllReplicationMetadataSchemas(storeName));
      String expectedSchema = "{\"type\":\"record\",\"name\":\"string_MetadataRecord\",\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"timestamp\",\"type\":[\"long\"],\"doc\":\"timestamp when the full record was last updated\",\"default\":0},{\"name\":\"replication_checkpoint_vector\",\"type\":{\"type\":\"array\",\"items\":\"long\"},\"doc\":\"high watermark remote checkpoints which touched this record\",\"default\":[]}]}";
      Assert.assertEquals(schemaResponse.getSchemas()[0].getSchemaStr(), expectedSchema);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testAAReplicationCanResolveConflicts(boolean useLogicalTimestamp, CompressionStrategy compressionStrategy) {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
      updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(false), compressionStrategy);

      // Empty push to create a version
      parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);

      // Verify that version 1 is already created in dc-0 region
      try (ControllerClient childControllerClient = new ControllerClient(clusterName, childDatacenters.get(0).getLeaderController(clusterName).getControllerUrl())) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = TestUtils.assertCommand(childControllerClient.getStore(storeName));
          Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
        });
      }
    }

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
    // Timestamp for Key2 with the highest offset, which will be used to verify that all messages in RT have been processed
    mockTimestampInMs.add(baselineTimestampInMs);
    Time mockTime = new MockCircularTime(mockTimestampInMs);
    String key1 = "key1";
    String value1 = "value1";
    String key2 = "key2";
    String value2 = "value2";

    // Build the SystemProducer with the mock time
    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
    try (VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(childDataCenter.getZkServerWrapper().getAddress(), SERVICE_NAME, storeName,
        Version.PushType.STREAM, Utils.getUniqueString("venice-push-id"), "dc-0", true, null, Optional.empty(),
        Optional.empty(), mockTime)) {
      producerInDC0.start();

      // Send <Key1, Value1>
      OutgoingMessageEnvelope envelope1 = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key1,
          useLogicalTimestamp ? new VeniceObjectWithTimestamp(value1, mockTime.getMilliseconds()) : value1);
      producerInDC0.send(storeName, envelope1);

      // Send <Key1, Value2>, which will be ignored by Servers if DCR is properly supported
      OutgoingMessageEnvelope envelope2 = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key1,
          useLogicalTimestamp ? new VeniceObjectWithTimestamp(value2, mockTime.getMilliseconds()) : value2);
      producerInDC0.send(storeName, envelope2);

      // Send <Key1, Value1> with same timestamp to trigger direct object comparison
      producerInDC0.send(storeName, envelope1);

      // Send <Key2, Value1>, which is used to verify that servers have consumed and processed till the end of all real-time messages
      OutgoingMessageEnvelope envelope3 = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key2,
          useLogicalTimestamp ? new VeniceObjectWithTimestamp(value1, mockTime.getMilliseconds()) : value1);
      producerInDC0.send(storeName, envelope3);
    }

    // Verify data in dc-0
    String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
    try (AvroGenericStoreClient<String, Object> client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Check <Key2, Value1> has been consumed
        Object valueObject = client.get(key2).get();
        Assert.assertNotNull(valueObject);
        Assert.assertEquals(valueObject.toString(), value1);
        // Check <Key1, Value2> was dropped, so that Key1 will have value equal to Value1
        Object valueObject1 = client.get(key1).get();
        Assert.assertNotNull(valueObject1);
        Assert.assertEquals(valueObject1.toString(), value1, "DCR is not working properly");
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
    // Timestamp for Key1 with a different value from dc-1 region; it will be consumed later than all messages in dc-0,
    // but since it has an older timestamp, its value will not override the previous value even though it will arrive at dc-0 servers later
    mockTimestampInMs.add(baselineTimestampInMs - 5);
    // Timestamp for Key3 with the highest offset in dc-1 RT, which will be used to verify that all messages in dc-1 RT have been processed
    mockTimestampInMs.add(baselineTimestampInMs);
    mockTime = new MockCircularTime(mockTimestampInMs);
    String key3 = "key3";
    String value3 = "value3";

    // Build the SystemProducer with the mock time
    VeniceMultiClusterWrapper childDataCenter1 = childDatacenters.get(1);
    try (VeniceSystemProducer producerInDC1 = new VeniceSystemProducer(childDataCenter1.getZkServerWrapper().getAddress(), SERVICE_NAME, storeName,
        Version.PushType.STREAM, Utils.getUniqueString("venice-push-id"), "dc-1", true, null, Optional.empty(),
        Optional.empty(), mockTime)) {
      producerInDC1.start();

      // Send <Key1, Value3>, which will be ignored if DCR is implemented properly
      OutgoingMessageEnvelope envelope4 = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key1,
          useLogicalTimestamp ? new VeniceObjectWithTimestamp(value3, mockTime.getMilliseconds()) : value3);
      producerInDC1.send(storeName, envelope4);

      // Send <Key3, Value1>, which is used to verify that servers have consumed and processed till the end of all real-time messages from dc-1
      OutgoingMessageEnvelope envelope5 = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key3,
          useLogicalTimestamp ? new VeniceObjectWithTimestamp(value1, mockTime.getMilliseconds()) : value1);
      producerInDC1.send(storeName, envelope5);
    }

    // Verify data in dc-0
    try (AvroGenericStoreClient<String, Object> client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Check <Key3, Value1> has been consumed
        Object valueObject = client.get(key3).get();
        Assert.assertNotNull(valueObject);
        Assert.assertEquals(valueObject.toString(), value1);
        // Check <Key1, Value3> was dropped, so that Key1 will have value equal to Value1
        Object valueObject1 = client.get(key1).get();
        Assert.assertNotNull(valueObject1);
        Assert.assertEquals(valueObject1.toString(), value1, "DCR is not working properly");
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testAAInOneDCWithHybridAggregateMode() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("hybridAA-test-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    int batchDataRangeEnd = 10;
    int overlapDataRangeStart = 5;
    int streamDataRangeEnd = 15;
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
        ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client, dc2Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
      TestUtils.verifySystemStoreInAllRegions(storeName, VeniceSystemStoreType.META_STORE, parentControllerClient, dcControllerClientList);
      TestUtils.verifySystemStoreInAllRegions(storeName, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, parentControllerClient, dcControllerClientList);
      TestUtils.assertCommand(parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams()
          .setLeaderFollowerModel(true)
          .setHybridRewindSeconds(10)
          .setHybridOffsetLagThreshold(2)
          .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE)));
      TestUtils.assertCommand(
          updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(false), Optional.of(false), CompressionStrategy.NO_OP));
      // Enable A/A in just one of the data center
      TestUtils.assertCommand(parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams()
          .setActiveActiveReplicationEnabled(true)
          .setRegionsFilter("dc-0,parent.parent")));
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc0Client, storeName, true, true);
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc1Client, storeName, true, false);
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc2Client, storeName, true, false);
      // Write some batch data, value would be the same as the key.
      VersionCreationResponse response = TestUtils.createVersionWithBatchData(parentControllerClient, storeName,
          STRING_SCHEMA, STRING_SCHEMA, IntStream.range(0, batchDataRangeEnd)
              .mapToObj(i -> new AbstractMap.SimpleEntry<>(String.valueOf(i), String.valueOf(i))), 1);
      TestUtils.waitForNonDeterministicPushCompletion(response.getKafkaTopic(), parentControllerClient, 60,
          TimeUnit.SECONDS, Optional.empty());
      Map<String, String> samzaConfig = new HashMap<>();
      String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
      samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
      samzaConfig.put(configPrefix + VENICE_STORE, storeName);
      samzaConfig.put(configPrefix + VENICE_AGGREGATE, "true");
      samzaConfig.put(D2_ZK_HOSTS_PROPERTY, "invalid_child_zk_address");
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, parentController.getKafkaZkAddress());
      samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
      samzaConfig.put(SSL_ENABLED, "false");
      VeniceSystemFactory factory = new VeniceSystemFactory();
      try (VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
        veniceProducer.start();

        for (int i = overlapDataRangeStart; i < streamDataRangeEnd; i++) {
          sendStreamingRecord(veniceProducer, storeName, i);
        }
      }
    }
    // Verify that all data centers should eventually have the same k/v.
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      String routerUrl = childDatacenters.get(dataCenterIndex).getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        final int dataCenterId = dataCenterIndex;
        // Verify batch only data
        for (int i = 0; i < overlapDataRangeStart; i++) {
          Object v = client.get(String.valueOf(i)).get();
          Assert.assertNotNull(v, "Batch data should have be consumed already in data center: " + dataCenterId);
          Assert.assertEquals(v.toString(), String.valueOf(i));
        }
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          for (int i = overlapDataRangeStart; i < streamDataRangeEnd; i++) {
            Object v = client.get(String.valueOf(i)).get();
            Assert.assertNotNull(v, "Servers in data center: " + dataCenterId + " haven't consumed real-time data yet");
            Assert.assertEquals(v.toString(), "stream_" + i);
          }
        });
      }
      VeniceServerWrapper serverWrapper = childDatacenters.get(dataCenterIndex).getClusters().get(clusterName).getVeniceServers().get(0);
      Map<String, ? extends Metric> metrics = serverWrapper.getMetricsRepository().metrics();
      metrics.forEach( (mName, metric) -> {
        if (mName.startsWith(String.format(".%s_current--rmd_disk_usage_in_bytes.", storeName))) {
          double value = metric.value();
          Assert.assertNotEquals(value, (double) StatsErrorCode.NULL_BDB_ENVIRONMENT.code, "Got a NULL_BDB_ENVIRONMENT!");
          Assert.assertNotEquals(value, (double) StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code,
              "Got NULL_STORAGE_ENGINE_STATS!");
          logger.info("DISK RMD usage " + value);
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHelixReplicationFactorConfigChange() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    VeniceClusterWrapper clusterForDC0Region = childDatacenters.get(0).getClusters().get(clusterName);
    String kafkaTopic;

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
      updateStore(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(true), CompressionStrategy.NO_OP);
      // Empty push to create a version
      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
      kafkaTopic = response.getKafkaTopic();
      // Verify that version 1 is already created in dc-0 region, and there are less than 3 ready-to-serve instances
      try (ControllerClient childControllerClient =
          new ControllerClient(clusterName, clusterForDC0Region.getLeaderVeniceController().getControllerUrl())) {
        OnlineInstanceFinder onlineInstanceFinder = clusterForDC0Region.getRandomVeniceRouter().getOnlineInstanceFinder();
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = TestUtils.assertCommand(childControllerClient.getStore(storeName));
          Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
          List<Instance> instances = onlineInstanceFinder.getReadyToServeInstances(kafkaTopic, 0);
          Assert.assertTrue(instances.size() < 3);
        });
      }
    }

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
      OnlineInstanceFinder onlineInstanceFinder = clusterForDC0Region.getRandomVeniceRouter().getOnlineInstanceFinder();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        List<Instance> instances = onlineInstanceFinder.getReadyToServeInstances(kafkaTopic, 0);
        Assert.assertEquals(instances.size(), 3);
      });
    } finally {
      if (server != null) {
        clusterForDC0Region.removeVeniceServer(server.getPort());
      }
      if (helixAdminForDC0Region != null) {
        helixAdminForDC0Region.close();
      }
    }
  }

  static ControllerResponse updateStore(
      String storeName,
      ControllerClient parentControllerClient,
      Optional<Boolean> enableNativeReplication,
      Optional<Boolean> enableActiveActiveReplication,
      Optional<Boolean> enableChunking,
      CompressionStrategy compressionStrategy
  ) {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setCompressionStrategy(compressionStrategy)
        .setLeaderFollowerModel(true);

    enableNativeReplication.ifPresent(params::setNativeReplicationEnabled);
    enableActiveActiveReplication.ifPresent(params::setActiveActiveReplicationEnabled);
    enableChunking.ifPresent(params::setChunkingEnabled);

    return parentControllerClient.updateStore(storeName, params);
  }

  public static void verifyDCConfigAARepl(ControllerClient controllerClient, String storeName, boolean isHybrid, boolean currentStatus, boolean expectedStatus) {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      Assert.assertEquals(storeResponse.getStore().isActiveActiveReplicationEnabled(), expectedStatus, "The active active replication config does not match.");
      if (isHybrid && (currentStatus != expectedStatus)) {
        DataReplicationPolicy policy = storeResponse.getStore().getHybridStoreConfig().getDataReplicationPolicy();
        Assert.assertEquals(policy, DataReplicationPolicy.NON_AGGREGATE, "The active active replication policy does not match.");
      }
    });
  }
}
