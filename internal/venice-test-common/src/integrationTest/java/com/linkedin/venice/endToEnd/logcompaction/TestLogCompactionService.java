package com.linkedin.venice.endToEnd.logcompaction;

import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.REPUSH_ORCHESTRATOR_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.SCHEDULED_LOG_COMPACTION_INTERVAL_MS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;

import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for {@link VeniceController} log compaction service.
 */
public class TestLogCompactionService {
  private static final int TEST_TIMEOUT = 60_000; // ms
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";
  private static final long TEST_LOG_COMPACTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

  private VeniceController testChildController;

  @BeforeClass
  public void setUp() {
    // config custom no-op test RepushOrchestrator implementation class
    Properties extraProperties = new Properties();
    extraProperties.setProperty(REPUSH_ORCHESTRATOR_CLASS_NAME, TestRepushOrchestratorImpl.class.getCanonicalName());
    extraProperties.setProperty(LOG_COMPACTION_ENABLED, "true");
    extraProperties.setProperty(SCHEDULED_LOG_COMPACTION_INTERVAL_MS, String.valueOf(TEST_LOG_COMPACTION_INTERVAL_MS));

    // create test controller
    ZkServerWrapper zkServer = ServiceFactory.getZkServer();
    PubSubBrokerWrapper pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).setRegionName(STANDALONE_REGION_NAME).build());

    VeniceControllerWrapper testChildControllerWrapper = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(CLUSTER_NAME, zkServer, pubSubBrokerWrapper)
            .extraProperties(extraProperties)
            .regionName(STANDALONE_REGION_NAME)
            .build());
    testChildController = testChildControllerWrapper.getController();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testScheduledLogCompaction() {
    // TODO LC: create fake stores. first one log-compaction-eligible but throws exception, second one
    // log-compaction-eligible, other two not log-compaction-eligible
    // String batchStoreName = Utils.getUniqueString("testStoreGraveyardCleanupBatch");
    // NewStoreResponse newStoreResponse =
    // parentControllerClient.createNewStore(batchStoreName, "test", "\"string\"", "\"string\"");
    // Assert.assertFalse(newStoreResponse.isError());

    // TODO LC: countdown latch
    // TODO LC: assert right store is repushed (check storename)
  }
}
