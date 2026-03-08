package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.blobtransfer.DaVinciBlobFinder;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * Shared test fixture encapsulating a VeniceClusterWrapper + D2Client for DaVinci integration tests.
 * Implements AutoCloseable for use in @BeforeClass/@AfterClass.
 */
public class DaVinciClusterFixture implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClusterFixture.class);

  private final VeniceClusterWrapper cluster;
  private final D2Client d2Client;
  private final PubSubProducerAdapterFactory pubSubProducerAdapterFactory; // nullable

  public DaVinciClusterFixture(boolean withPubSubProducer) {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    VeniceClusterCreateOptions.Builder builder = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(1)
        .replicationFactor(2)
        .extraProperties(clusterConfig);
    if (withPubSubProducer) {
      builder.partitionSize(100).sslToStorageNodes(false).sslToKafka(false);
    }
    cluster = ServiceFactory.getVeniceCluster(builder.build());
    d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
    pubSubProducerAdapterFactory = withPubSubProducer
        ? cluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory()
        : null;
  }

  public DaVinciClusterFixture() {
    this(false);
  }

  public VeniceClusterWrapper getCluster() {
    return cluster;
  }

  public D2Client getD2Client() {
    return d2Client;
  }

  public PubSubProducerAdapterFactory getPubSubProducerAdapterFactory() {
    return pubSubProducerAdapterFactory;
  }

  /**
   * Creates a store along with its meta system store and push status system store.
   */
  public String createStoreWithSystemStores(int keyCount) throws Exception {
    String storeName = cluster.createStore(keyCount);
    cluster.createMetaSystemStore(storeName);
    cluster.createPushStatusSystemStore(storeName);
    return storeName;
  }

  /**
   * Polls the router's blob discovery endpoint until at least one peer is discovered
   * for the given store/version/partition. Call this after the forked DaVinciUserApp
   * signals ready but before starting a second DaVinci client, to ensure the first
   * client's push status has propagated through the system store.
   */
  public static void waitForBlobPeerDiscovery(D2Client d2Client, String storeName, int version, int partition) {
    ClientConfig clientConfig = new ClientConfig(storeName).setD2Client(d2Client)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setMetricsRepository(new MetricsRepository());
    try (DaVinciBlobFinder blobFinder = new DaVinciBlobFinder(clientConfig)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        BlobPeersDiscoveryResponse response = blobFinder.discoverBlobPeers(storeName, version, partition);
        Assert.assertNotNull(response, "Blob discovery response should not be null");
        Assert.assertFalse(response.isError(), "Blob discovery returned error: " + response.getErrorMessage());
        List<String> peers = response.getDiscoveryResult();
        Assert.assertFalse(peers.isEmpty(), "Expected at least one blob peer for " + storeName + "_v" + version);
      });
      LOGGER.info("Blob peer discovered for {} v{} partition {}", storeName, version, partition);
    } catch (Exception e) {
      throw new RuntimeException("Failed waiting for blob peer discovery", e);
    }
  }

  @Override
  public void close() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }
}
