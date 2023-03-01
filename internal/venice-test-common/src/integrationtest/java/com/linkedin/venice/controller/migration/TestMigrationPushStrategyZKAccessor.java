package com.linkedin.venice.controller.migration;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.migration.MigrationPushStrategy;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestMigrationPushStrategyZKAccessor {
  private ZkServerWrapper zkServer;
  private ZkClient zkClient;

  @BeforeMethod
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServer.getAddress());
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zkServer.close();
  }

  @Test
  public void testCreateAndUpdatePushStrategy() {
    MigrationPushStrategyZKAccessor accessor =
        new MigrationPushStrategyZKAccessor(zkClient, new HelixAdapterSerializer());
    String voldemortStore = Utils.getUniqueString("voldemort_store");
    accessor.setPushStrategy(voldemortStore, MigrationPushStrategy.RunBnPAndVPJWaitForBothStrategy.name());
    Map<String, String> pushStrategies = accessor.getAllPushStrategies();
    Assert.assertTrue(pushStrategies.containsKey(voldemortStore));
    Assert
        .assertEquals(pushStrategies.get(voldemortStore), MigrationPushStrategy.RunBnPAndVPJWaitForBothStrategy.name());
    // update
    accessor.setPushStrategy(voldemortStore, MigrationPushStrategy.RunBnPOnlyStrategy.name());
    pushStrategies = accessor.getAllPushStrategies();
    Assert.assertTrue(pushStrategies.containsKey(voldemortStore));
    Assert.assertEquals(pushStrategies.get(voldemortStore), MigrationPushStrategy.RunBnPOnlyStrategy.name());
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSetInvalidStrategy() {
    MigrationPushStrategyZKAccessor accessor =
        new MigrationPushStrategyZKAccessor(zkClient, new HelixAdapterSerializer());
    String voldemortStore = Utils.getUniqueString("voldemort_store");
    accessor.setPushStrategy(voldemortStore, "invalid_strategy");
  }
}
