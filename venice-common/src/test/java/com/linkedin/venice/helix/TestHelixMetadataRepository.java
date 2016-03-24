package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixMetadataRepository. All the tests depend on Zookeeper. Please ensure there is a zookeeper
 * available for testing.
 */
public class TestHelixMetadataRepository {
    private String zkAddress;
    private ZkClient zkClient;
    private String cluster = "test-metadata-cluster";
    private String clusterPath = "/test-metadata-cluster";
    private String storesPath = "/stores";
    private ZkServerWrapper zkServerWrapper;
    private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

    @BeforeMethod
    public void zkSetup() {
        zkServerWrapper = ServiceFactory.getZkServer();
        zkAddress = zkServerWrapper.getAddress();
        zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, adapter);
        zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
        zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);
    }

    @AfterMethod
    public void zkCleanup() {
        zkClient.deleteRecursive(clusterPath);
        zkClient.close();
        zkServerWrapper.close();
    }

    @Test
    public void testAddAndReadStore() {
        HelixMetadataRepository repo = new HelixMetadataRepository(zkClient, adapter, cluster);
        Store s1 = new Store("s1", "owner", System.currentTimeMillis());
        repo.addStore(s1);
        Store s2 = repo.getStore("s1");
        Assert.assertEquals(s2, s1, "Store get from ZK is different with local one");
    }

    @Test
    public void testAddAndDeleteStore() {
        HelixMetadataRepository repo = new HelixMetadataRepository(zkClient, adapter, cluster);
        Store s1 = new Store("s1", "owner", System.currentTimeMillis());
        repo.addStore(s1);
        repo.deleteStore("s1");
        Assert.assertNull(repo.getStore("s1"));
    }
}
