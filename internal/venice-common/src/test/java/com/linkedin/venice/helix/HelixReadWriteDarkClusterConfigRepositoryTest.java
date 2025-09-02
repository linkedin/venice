package com.linkedin.venice.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.DarkClusterConfig;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixReadWriteDarkClusterConfigRepositoryTest {
  private ZkClient zkClient;
  private HelixAdapterSerializer adapter;
  private HelixReadWriteDarkClusterConfigRepository repo;
  private ZkBaseDataAccessor<DarkClusterConfig> accessor;
  private String clusterConfigZkPath;
  private final static String clusterName = "testCluster";

  @BeforeMethod
  public void setUp() {
    zkClient = mock(ZkClient.class);
    adapter = new HelixAdapterSerializer();
    doNothing().when(zkClient).setZkSerializer(any(HelixAdapterSerializer.class));
    accessor = mock(ZkBaseDataAccessor.class);

    repo = new HelixReadWriteDarkClusterConfigRepository(zkClient, adapter, clusterName);
    clusterConfigZkPath = repo.getClusterConfigZkPath();
    repo.setZkDataAccessor(accessor);
  }

  @Test
  public void testUpdateConfigs() {
    DarkClusterConfig config = new DarkClusterConfig();
    config.getStoresToReplicate().add("storeA");
    doReturn(true).when(accessor).set(anyString(), any(), anyInt());

    repo.updateConfigs(config);

    verify(accessor, times(1)).set(eq(clusterConfigZkPath), eq(config), eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testDeleteConfigs() {
    doReturn(true).when(accessor).remove(anyString(), anyInt());

    repo.deleteConfigs();

    verify(accessor, times(1)).remove(eq(clusterConfigZkPath), eq(AccessOption.PERSISTENT));
  }
}
