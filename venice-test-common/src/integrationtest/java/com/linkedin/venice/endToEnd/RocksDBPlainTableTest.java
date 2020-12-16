package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.Time;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;


public class RocksDBPlainTableTest {
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass
  public void setup() {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 0, 1);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(veniceCluster);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testInvalidPlainTableConfig() {
    Properties serverProperties = getRocksDBPlainTableEnabledProperties();
    serverProperties.put(ROCKSDB_OPTIONS_USE_DIRECT_READS, true);
    Assert.assertThrows(VeniceException.class, () -> {
      veniceCluster.addVeniceServer(new Properties(), serverProperties);
    });
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPlainTableEndToEnd() throws Exception {
    veniceCluster.addVeniceServer(new Properties(), getRocksDBPlainTableEnabledProperties());
    final int keyCount = 100;
    String storeName = veniceCluster.createStore(keyCount);
    try (AvroGenericStoreClient<Integer, Integer> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 0; i < keyCount; ++i) {
        Integer value = client.get(i).get();
        Assert.assertNotNull(value);
      }
    }
  }

  private Properties getRocksDBPlainTableEnabledProperties() {
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, true);
    serverProperties.put(ROCKSDB_OPTIONS_USE_DIRECT_READS, false);
    return serverProperties;
  }
}
