package com.linkedin.venice.controller.migration;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.migration.MigrationPushStrategy;
import com.linkedin.venice.utils.TestUtils;
import java.util.Map;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestMigrationPushStrategyZKAccessor {
  private ZkServerWrapper zkServer;
  private ZkClient zkClient;
  private static final int WAIT_TIME = 10000;

  @BeforeMethod
  public void setup() {
    zkServer = ServiceFactory.getZkServer();
    zkClient = new ZkClient(zkServer.getAddress(), WAIT_TIME);
  }

  @AfterMethod
  public void cleanup() {
    zkClient.close();
    zkServer.close();
  }

  @Test
  public void testCreateAndUpdatePushStrategy() {
    MigrationPushStrategyZKAccessor accessor = new MigrationPushStrategyZKAccessor(zkClient, new HelixAdapterSerializer());
    String voldemortStore = TestUtils.getUniqueString("voldemort_store");
    accessor.setPushStrategy(voldemortStore, MigrationPushStrategy.RunBnPAndH2VWaitForBothStrategy.name());
    Map<String, String> pushStrategies = accessor.getAllPushStrategies();
    Assert.assertTrue(pushStrategies.containsKey(voldemortStore));
    Assert.assertEquals(pushStrategies.get(voldemortStore), MigrationPushStrategy.RunBnPAndH2VWaitForBothStrategy.name());
    // update
    accessor.setPushStrategy(voldemortStore, MigrationPushStrategy.RunBnPOnlyStrategy.name());
    pushStrategies = accessor.getAllPushStrategies();
    Assert.assertTrue(pushStrategies.containsKey(voldemortStore));
    Assert.assertEquals(pushStrategies.get(voldemortStore), MigrationPushStrategy.RunBnPOnlyStrategy.name());
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testSetInvalidStrategy() {
    MigrationPushStrategyZKAccessor accessor = new MigrationPushStrategyZKAccessor(zkClient, new HelixAdapterSerializer());
    String voldemortStore = TestUtils.getUniqueString("voldemort_store");
    accessor.setPushStrategy(voldemortStore, "invalid_strategy");
  }
}
