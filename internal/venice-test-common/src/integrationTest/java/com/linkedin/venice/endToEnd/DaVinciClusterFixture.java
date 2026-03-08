package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


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
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
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

  @Override
  public void close() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }
}
