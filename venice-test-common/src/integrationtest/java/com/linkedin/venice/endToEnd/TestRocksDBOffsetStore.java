package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


public class TestRocksDBOffsetStore {
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
  public void testStorageMetadataServiceOffsets() throws Exception {
    VeniceServerWrapper serverWrapper = veniceCluster.addVeniceServer(new Properties(), getRocksDBOffsetStoreEnabledProperties());
    final int keyCount = 100;
    String storeName = veniceCluster.createStore(keyCount);
    String storeTopicName = storeName + "_v1";
    StorageMetadataService storageMetadataService = serverWrapper.getVeniceServer().getStorageMetadataService();
    Assert.assertTrue(storageMetadataService.getLastOffset(storeTopicName, 0).getLocalVersionTopicOffset() != -1);
    veniceCluster.stopVeniceServer(serverWrapper.getPort());
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> veniceCluster.getRandomVeniceRouter()
        .getRoutingDataRepository().getPartitionAssignments(storeTopicName).getAssignedNumberOfPartitions() == 0);    veniceCluster.restartVeniceServer(serverWrapper.getPort());
    storageMetadataService = veniceCluster.getVeniceServers().get(0).getVeniceServer().getStorageMetadataService();
    Assert.assertTrue(storageMetadataService.getLastOffset(storeTopicName, 0).getLocalVersionTopicOffset() != -1);
    try (AvroGenericStoreClient<Integer, Integer> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 0; i < keyCount; ++i) {
        Integer value = client.get(i).get();
        Assert.assertNotNull(value);
      }
    }
  }

  private Properties getRocksDBOffsetStoreEnabledProperties() {
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    return serverProperties;
  }
}
