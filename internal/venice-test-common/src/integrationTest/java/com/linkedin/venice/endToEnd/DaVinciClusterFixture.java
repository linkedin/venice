package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;


/**
 * Shared test fixture encapsulating a VeniceClusterWrapper + D2Client for DaVinci integration tests.
 * Implements AutoCloseable for use in @BeforeClass/@AfterClass.
 */
public class DaVinciClusterFixture implements AutoCloseable {
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

  @Override
  public void close() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }
}
