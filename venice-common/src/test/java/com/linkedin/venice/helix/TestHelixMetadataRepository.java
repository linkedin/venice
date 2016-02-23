package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


/**
 * Test cases for HelixMetadataRepository. All the tests depend on Zookeeper. Please ensure there is a zookeeper
 * available for testing.
 */
public class TestHelixMetadataRepository {
    private String zkAddress = "localhost:2181";
    private ZkClient zkClient;
    private String clusterPath = "/test-metadata-cluster";
    private String storesPath = "/stores";
    /**
     * By default, this test is inactive. Because it depends on external zk process. It should be only used in
     * debugging.
     */
    private final boolean isEnable = false;

    @BeforeTest(enabled = isEnable)
    public void zkSetup() {
        zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new HelixStoreSerializer(new StoreJSONSerializer()));
        zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
        zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);
    }

    @AfterTest(enabled = isEnable)
    public void zkCleanup() {
        zkClient.deleteRecursive(clusterPath);
        zkClient.delete(clusterPath);
        zkClient.close();
    }

    @Test(enabled = isEnable)
    public void testAddAndReadStore() {
        HelixMetadataRepository repo = new HelixMetadataRepository(zkClient, clusterPath + storesPath);
        Store s1 = new Store("s1", "owner", System.currentTimeMillis());
        repo.addStore(s1);
        Store s2 = repo.getStore("s1");
        Assert.assertEquals(s1, s2);
    }

    @Test(enabled = isEnable)
    public void testAddAndDeleteStore() {
        HelixMetadataRepository repo = new HelixMetadataRepository(zkClient, clusterPath + storesPath);
        Store s1 = new Store("s1", "owner", System.currentTimeMillis());
        repo.addStore(s1);
        repo.deleteStore("s1");
        Assert.assertNull(repo.getStore("s1"));
    }
}
