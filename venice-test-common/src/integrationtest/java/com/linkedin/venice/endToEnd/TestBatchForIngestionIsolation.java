package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.TestUtils;
import java.util.Properties;
import org.testng.annotations.Test;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;


@Test(singleThreaded = true)
public class TestBatchForIngestionIsolation extends TestBatch {

  @Override
  public VeniceClusterWrapper initializeVeniceCluster() {
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1, 0, 0);

    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    TestUtils.addIngestionIsolationToProperties(serverProperties);
    serverProperties.setProperty(DATA_BASE_PATH, baseDataPath1);
    veniceClusterWrapper.addVeniceServer(new Properties(), serverProperties);
    serverProperties.setProperty(DATA_BASE_PATH, baseDataPath2);
    veniceClusterWrapper.addVeniceServer(new Properties(), serverProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceClusterWrapper.addVeniceRouter(routerProperties);

    return veniceClusterWrapper;
  }
}