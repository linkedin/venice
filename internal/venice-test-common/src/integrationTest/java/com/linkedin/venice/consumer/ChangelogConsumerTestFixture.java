package com.linkedin.venice.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Shared test fixture for changelog consumer integration tests.
 * Encapsulates multi-region cluster setup with configurable parallel processing.
 */
public class ChangelogConsumerTestFixture implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(ChangelogConsumerTestFixture.class);
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private final VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private final List<VeniceMultiClusterWrapper> childDatacenters;
  private final List<VeniceControllerWrapper> parentControllers;
  private final String clusterName;
  private final VeniceClusterWrapper clusterWrapper;
  private final ControllerClient parentControllerClient;
  private final ControllerClient childControllerClientRegion0;
  private final D2Client d2Client;
  private final ZkServerWrapper localZkServer;
  private final PubSubBrokerWrapper localKafka;

  private final List<AutoCloseable> testCloseables = new ArrayList<>();
  private final List<String> testStoresToDelete = new ArrayList<>();

  public ChangelogConsumerTestFixture(boolean parallelProcessingEnabled) {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, parallelProcessingEnabled);

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
    localZkServer = childDatacenters.get(0).getZkServerWrapper();
    localKafka = childDatacenters.get(0).getPubSubBrokerWrapper();

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    childControllerClientRegion0 =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    d2Client = new D2ClientBuilder()
        .setZkHosts(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  public VeniceTwoLayerMultiRegionMultiClusterWrapper getMultiRegionMultiClusterWrapper() {
    return multiRegionMultiClusterWrapper;
  }

  public List<VeniceMultiClusterWrapper> getChildDatacenters() {
    return childDatacenters;
  }

  public List<VeniceControllerWrapper> getParentControllers() {
    return parentControllers;
  }

  public String getClusterName() {
    return clusterName;
  }

  public VeniceClusterWrapper getClusterWrapper() {
    return clusterWrapper;
  }

  public ControllerClient getParentControllerClient() {
    return parentControllerClient;
  }

  public ControllerClient getChildControllerClientRegion0() {
    return childControllerClientRegion0;
  }

  public D2Client getD2Client() {
    return d2Client;
  }

  public ZkServerWrapper getLocalZkServer() {
    return localZkServer;
  }

  public PubSubBrokerWrapper getLocalKafka() {
    return localKafka;
  }

  public List<AutoCloseable> getTestCloseables() {
    return testCloseables;
  }

  public List<String> getTestStoresToDelete() {
    return testStoresToDelete;
  }

  public void addCloseable(AutoCloseable closeable) {
    testCloseables.add(closeable);
  }

  public void addStoreToDelete(String storeName) {
    testStoresToDelete.add(storeName);
  }

  public void cleanupAfterTest() {
    ChangelogConsumerTestUtils.cleanupAfterTest(testCloseables, testStoresToDelete, parentControllerClient, LOGGER);
  }

  @Override
  public void close() {
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(childControllerClientRegion0);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
    TestView.resetCounters();
  }
}
