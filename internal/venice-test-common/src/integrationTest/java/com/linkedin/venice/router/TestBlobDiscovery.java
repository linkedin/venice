package com.linkedin.venice.router;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.utils.TestUtils.assertCommand;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.blobtransfer.DaVinciBlobFinder;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBlobDiscovery {
  private static final Logger LOGGER = LogManager.getLogger(TestBlobDiscovery.class);
  private static final String INT_KEY_SCHEMA = "\"int\"";
  private static final String INT_VALUE_SCHEMA = "\"int\"";
  String clusterName;
  String clusterName2;
  String storeName;
  String storeName2;
  private VeniceMultiClusterWrapper multiClusterVenice;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
  D2Client daVinciD2;

  /**
   * Set up a multi-cluster Venice environment with meta system store enabled Venice stores.
   */

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();

    Properties props = new Properties();
    props.put(OFFLINE_JOB_START_TIMEOUT_MS, "180000");

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(2)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(3)
            .numberOfRouters(1)
            .replicationFactor(3)
            .forkServer(false)
            .parentControllerProperties(props);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    pubSubPositionTypeRegistry =
        multiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper().getPubSubPositionTypeRegistry();

    multiClusterVenice = multiRegionMultiClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterVenice.getClusterNames();
    String parentControllerURLs = multiRegionMultiClusterWrapper.getParentControllers()
        .stream()
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.joining(","));

    clusterName = clusterNames[0];
    clusterName2 = clusterNames[1];
    storeName = Utils.getUniqueString("test-store");
    storeName2 = Utils.getUniqueString("test-store");
    LOGGER.info(
        "Cluster 1 is {}, Cluster 2 is {}, Store 1 is {}, Store 2 is {}",
        clusterName,
        clusterName2,
        storeName,
        storeName2);

    List<PubSubBrokerWrapper> pubSubBrokerWrappers = multiClusterVenice.getClusters()
        .values()
        .stream()
        .map(VeniceClusterWrapper::getPubSubBrokerWrapper)
        .collect(Collectors.toList());
    Map<String, String> additionalPubSubProperties =
        PubSubBrokerWrapper.getBrokerDetailsForClients(pubSubBrokerWrappers);

    // create store 1 in cluster 1
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      assertCommand(parentControllerClient.createNewStore(storeName, "venice-test", INT_KEY_SCHEMA, INT_VALUE_SCHEMA));

      PubSubProducerAdapterFactory pubSubProducerAdapterFactory = multiClusterVenice.getClusters()
          .get(clusterName)
          .getPubSubBrokerWrapper()
          .getPubSubClientsFactory()
          .getProducerAdapterFactory();

      VersionCreationResponse response = TestUtils.createVersionWithBatchData(
          parentControllerClient,
          storeName,
          INT_KEY_SCHEMA,
          INT_VALUE_SCHEMA,
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, 0)),
          pubSubProducerAdapterFactory,
          additionalPubSubProperties,
          pubSubPositionTypeRegistry);

      // Verify the data can be ingested by classical Venice before proceeding.
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      makeSureSystemStoresAreOnline(parentControllerClient, storeName);
      multiClusterVenice.getClusters().get(clusterName).refreshAllRouterMetaData();
    }

    // create store 2 in cluster 2
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName2, parentControllerURLs)) {
      assertCommand(parentControllerClient.createNewStore(storeName2, "venice-test", INT_KEY_SCHEMA, INT_VALUE_SCHEMA));

      PubSubProducerAdapterFactory pubSubProducerAdapterFactory = multiClusterVenice.getClusters()
          .get(clusterName2)
          .getPubSubBrokerWrapper()
          .getPubSubClientsFactory()
          .getProducerAdapterFactory();

      VersionCreationResponse response = TestUtils.createVersionWithBatchData(
          parentControllerClient,
          storeName2,
          INT_KEY_SCHEMA,
          INT_VALUE_SCHEMA,
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, 0)),
          pubSubProducerAdapterFactory,
          additionalPubSubProperties,
          pubSubPositionTypeRegistry);

      // Verify the data can be ingested by classical Venice before proceeding.
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      makeSureSystemStoresAreOnline(parentControllerClient, storeName2);
      multiClusterVenice.getClusters().get(clusterName2).refreshAllRouterMetaData();
    }

    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .put(PUSH_STATUS_STORE_ENABLED, true)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 1)
            .build();

    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciD2 = D2TestUtils.getAndStartD2Client(multiClusterVenice.getZkServerWrapper().getAddress());

    // create one DVC client which subscribes to all partitions for store 1 and store 2
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        daVinciD2,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig)) {
      // subscribe to all partitions for store 1
      List<DaVinciClient<Integer, Object>> clients = new ArrayList<>();
      DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      client.subscribeAll().get();
      clients.add(client);
      // subscribe to all partitions for store 2
      DaVinciClient<Integer, Object> client2 = factory.getAndStartGenericAvroClient(storeName2, daVinciConfig);
      client2.subscribeAll().get();
      clients.add(client2);
      // This is a very dumb and basic assertion that's only purpose is to get static analysis to not be mad
      Assert.assertTrue(clients.get(0).getPartitionCount() > 0);
      Assert.assertTrue(clients.get(1).getPartitionCount() > 0);
    } catch (ExecutionException | InterruptedException e) {
      throw new VeniceException(e);
    }
  }

  @AfterTest
  public void tearDown() {
    D2ClientUtils.shutdownClient(daVinciD2);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testBlobDiscovery() throws Exception {
    VeniceClusterWrapper veniceClusterWrapper = multiClusterVenice.getClusters().get(clusterName);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      veniceClusterWrapper.updateStore(storeName, new UpdateStoreQueryParams().setBlobTransferEnabled(true));
    });
    VeniceClusterWrapper veniceClusterWrapper2 = multiClusterVenice.getClusters().get(clusterName2);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      veniceClusterWrapper2.updateStore(storeName2, new UpdateStoreQueryParams().setBlobTransferEnabled(true));
    });

    // Even if the config here only use "storeName",
    // the DaVinciBlobFinder should be able to discover both stores
    ClientConfig clientConfig = new ClientConfig(storeName).setD2Client(daVinciD2)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setMetricsRepository(new MetricsRepository());

    // Checking if the DVC created at the beginning is found by the DaVinciBlobFinder
    // when try to find store1's peers.
    BlobFinder daVinciBlobFinder = new DaVinciBlobFinder(clientConfig);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      BlobPeersDiscoveryResponse response = daVinciBlobFinder.discoverBlobPeers(storeName, 1, 1);
      Assert.assertNotNull(response);
      List<String> hostNames = response.getDiscoveryResult();
      LOGGER.info("Discovered hosts: {} for store1 {}", hostNames, storeName);
      Assert.assertNotNull(hostNames);
      Assert.assertEquals(hostNames.size(), 1);
    });

    // Checking if the DVC created at the beginning is found by the DaVinciBlobFinder
    // when try to find store2's peers.
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      BlobPeersDiscoveryResponse response = daVinciBlobFinder.discoverBlobPeers(storeName2, 1, 1);
      Assert.assertNotNull(response);
      List<String> hostNames = response.getDiscoveryResult();
      LOGGER.info("Discovered hosts: {} for store2 {}", hostNames, storeName2);
      Assert.assertNotNull(hostNames);
      Assert.assertEquals(hostNames.size(), 1);
    });
  }

  private void makeSureSystemStoresAreOnline(ControllerClient controllerClient, String storeName) {
    String metaSystemStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1);
    TestUtils.waitForNonDeterministicPushCompletion(metaSystemStoreTopic, controllerClient, 30, TimeUnit.SECONDS);
    String daVinciPushStatusStore =
        Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);
    TestUtils.waitForNonDeterministicPushCompletion(daVinciPushStatusStore, controllerClient, 30, TimeUnit.SECONDS);
  }
}
