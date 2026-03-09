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
import com.linkedin.venice.utils.Time;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;


/**
 * Abstract base class for changelog consumer integration tests. Provides shared cluster setup,
 * teardown, and common fields. Concrete subclasses add test methods for specific feature areas.
 */
public abstract class AbstractChangelogConsumerTest {
  private static final Logger LOGGER = LogManager.getLogger(AbstractChangelogConsumerTest.class);
  protected static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  protected static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  protected final List<AutoCloseable> testCloseables = new ArrayList<>();
  protected final List<String> testStoresToDelete = new ArrayList<>();

  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  protected String clusterName;
  protected VeniceClusterWrapper clusterWrapper;
  protected ControllerClient parentControllerClient;
  protected ControllerClient childControllerClientRegion0;
  protected D2Client d2Client;
  protected ZkServerWrapper localZkServer;
  protected PubSubBrokerWrapper localKafka;

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, isAAWCParallelProcessingEnabled());

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

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(childControllerClientRegion0);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
    TestView.resetCounters();
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    ChangelogConsumerTestUtils.cleanupAfterTest(testCloseables, testStoresToDelete, parentControllerClient, LOGGER);
  }
}
