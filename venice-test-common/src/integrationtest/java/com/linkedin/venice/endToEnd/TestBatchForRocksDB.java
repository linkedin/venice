package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import java.util.Properties;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


@Test(singleThreaded = true)
public class TestBatchForRocksDB extends TestBatch {

  @Override
  public VeniceClusterWrapper initializeVeniceCluster() {
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1, 0, 0);

    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    veniceClusterWrapper.addVeniceServer(serverProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceClusterWrapper.addVeniceRouter(routerProperties);

    return veniceClusterWrapper;
  }
}
