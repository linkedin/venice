package com.linkedin.venice.endToEnd.logcompaction;

import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.REPUSH_ORCHESTRATOR_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.TIME_SINCE_LAST_LOG_COMPACTION_THRESHOLD_MS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.controller.repush.RepushJobResponse;
import com.linkedin.venice.controller.repush.RepushOrchestrator;
import com.linkedin.venice.controller.repush.RepushOrchestratorConfig;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for {@link VeniceController} log compaction service.
 */
public class TestLogCompactionService {
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";
  private static final int TEST_TIMEOUT = 999999; // ms
  private static final long TEST_LOG_COMPACTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
  private static final long TEST_TIME_SINCE_LAST_LOG_COMPACTION_THRESHOLD_MS = TimeUnit.SECONDS.toMillis(1);

  private VeniceControllerWrapper childControllerWrapper;
  private static CountDownLatch latch;

  @BeforeClass
  public void setUp() {
    // config custom no-op test RepushOrchestrator implementation class
    Properties extraProperties = new Properties();
    extraProperties.setProperty(REPUSH_ORCHESTRATOR_CLASS_NAME, TestRepushOrchestratorImpl.class.getName());
    extraProperties.setProperty(LOG_COMPACTION_ENABLED, "true");
    extraProperties.setProperty(LOG_COMPACTION_INTERVAL_MS, String.valueOf(TEST_LOG_COMPACTION_INTERVAL_MS));
    extraProperties.setProperty(
        TIME_SINCE_LAST_LOG_COMPACTION_THRESHOLD_MS,
        String.valueOf(TEST_TIME_SINCE_LAST_LOG_COMPACTION_THRESHOLD_MS));

    // create test controller
    ZkServerWrapper zkServer = ServiceFactory.getZkServer();
    PubSubBrokerWrapper pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).setRegionName(STANDALONE_REGION_NAME).build());

    childControllerWrapper = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(CLUSTER_NAME, zkServer, pubSubBrokerWrapper)
            .extraProperties(extraProperties)
            .regionName(STANDALONE_REGION_NAME)
            .build());
    TestUtils.waitForNonDeterministicCompletion(
        5,
        TimeUnit.SECONDS,
        () -> childControllerWrapper.isLeaderController(CLUSTER_NAME));

    latch = new CountDownLatch(1);
  }

  // TODO LC: extract TestHybrid::testHybridStoreLogCompaction() to this method
  @Test
  public void testScheduledLogCompaction() {

    // createStore() -> getStore() -> setHybridStoreConfig()
    Admin admin = childControllerWrapper.getVeniceAdmin();

    // create hybrid store -> eligible for log compaction
    String storeName = Utils.getUniqueString("test");
    admin.createStore(CLUSTER_NAME, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
    admin.updateStore(
        CLUSTER_NAME,
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(0).setHybridOffsetLagThreshold(0));

    Assert.assertTrue(admin.getStore(CLUSTER_NAME, storeName).isHybrid());
    Assert.assertFalse(admin.getStore(CLUSTER_NAME, storeName).containsVersion(1));
    Assert.assertEquals(admin.getStore(CLUSTER_NAME, storeName).getCurrentVersion(), 0);

    // Wait for the latch to count down
    try {
      if (latch.await(TEST_TIMEOUT, TimeUnit.MILLISECONDS)) {
        System.out.println("Log compaction job triggered");
      }
    } catch (InterruptedException e) {
      System.out.println("Log compaction job failed");
      throw new RuntimeException(e);
    }

    // // Verify that repush was called the expected number of timesCapture the argument passed to repush()
    // ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    // verify(mockRepushOrchestrator, times(expectedRepushInvocationCount)).repush(argumentCaptor.capture());
    //
    // // Assert the value of the captured argument
    // assertEquals(argumentCaptor.getValue(), "" /* TODO LC: store to be compacted */);
    Utils.sleep(20000L);
  }

  @AfterClass
  public void cleanUp() {
    childControllerWrapper.close();
  }

  public static class TestRepushOrchestratorImpl implements RepushOrchestrator {
    @Override
    public void init(RepushOrchestratorConfig config) {
      // no-op
    }

    @Override
    public RepushJobResponse repush(String storeName) {
      latch.countDown();
      System.out.println("Repush job triggered for store: " + storeName);
      return null;
    }
  }
}
