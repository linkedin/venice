package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkExecutionIdAccessor {
  private ZkClient zkClient;
  private ZkExecutionIdAccessor executionIdAccessor;
  private String clusterName = "TestZkExecutionIdAccessor";
  private ZkServerWrapper zkServer;

  @BeforeMethod
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServer.getAddress());
    zkClient.createPersistent(HelixUtils.getHelixClusterZkPath(clusterName));
    executionIdAccessor = new ZkExecutionIdAccessor(zkClient, new HelixAdapterSerializer());
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zkServer.close();
  }

  @Test
  public void getLastSucceedExecutionId() {
    long id = 100L;
    executionIdAccessor.updateLastSucceededExecutionId(clusterName, id);
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionId("non-existing-cluster"),
        Long.valueOf(-1),
        "Cluster has not been created.");
    Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName), Long.valueOf(id));
  }

  @Test
  public void getLastGeneratedExecutionId() {
    Assert.assertEquals(executionIdAccessor.incrementAndGetExecutionId(clusterName).longValue(), 0L);
    Assert.assertEquals(executionIdAccessor.incrementAndGetExecutionId(clusterName).longValue(), 1L);
  }
}
