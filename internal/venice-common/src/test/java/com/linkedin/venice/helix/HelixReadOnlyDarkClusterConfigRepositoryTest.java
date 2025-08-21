package com.linkedin.venice.helix;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.meta.DarkClusterConfig;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixReadOnlyDarkClusterConfigRepositoryTest {
  private ZkClient zkClient;
  private HelixAdapterSerializer adapter;
  private HelixReadOnlyDarkClusterConfigRepository repo;
  HelixReadOnlyDarkClusterConfigRepository.ClusterConfigZkListener listener;
  private final String clusterName = "testCluster";

  @BeforeMethod
  public void setUp() {
    zkClient = mock(ZkClient.class);
    adapter = mock(HelixAdapterSerializer.class);
    repo = new HelixReadOnlyDarkClusterConfigRepository(zkClient, adapter, clusterName);
    listener = repo.getClusterConfigZkListener();
  }

  @Test
  public void testDefaultConfigIsEmpty() {
    DarkClusterConfig config = repo.getConfigs();
    assertNotNull(config);
    assertTrue(config.getTargetStores().isEmpty());
  }

  @Test
  public void testClusterConfigListenerHandlesDataChangeAndDelete() {
    DarkClusterConfig config = new DarkClusterConfig();
    config.getTargetStores().add("storeA");
    config.getTargetStores().add("storeB");

    // Data change: should update config
    listener.handleDataChange("/path", config);
    assertEquals(repo.getConfigs().getTargetStores(), config.getTargetStores());

    // Data change with invalid type: should throw
    try {
      listener.handleDataChange("/path", new Object());
      fail("Expected VeniceException for invalid data");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid config data"));
    }

    // Data deleted: should reset config to default
    listener.handleDataDeleted("/path");
    assertTrue(repo.getConfigs().getTargetStores().isEmpty());
  }
}
