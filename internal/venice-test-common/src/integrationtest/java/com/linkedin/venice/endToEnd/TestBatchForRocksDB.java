package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import java.util.Properties;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestBatchForRocksDB extends TestBatch {
  @Override
  public VeniceClusterWrapper initializeVeniceCluster() {
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1, 0, 0);

    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.setProperty(DATA_BASE_PATH, BASE_DATA_PATH_1);
    veniceClusterWrapper.addVeniceServer(new Properties(), serverProperties);
    serverProperties.setProperty(DATA_BASE_PATH, BASE_DATA_PATH_2);
    veniceClusterWrapper.addVeniceServer(new Properties(), serverProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceClusterWrapper.addVeniceRouter(routerProperties);

    return veniceClusterWrapper;
  }
}
