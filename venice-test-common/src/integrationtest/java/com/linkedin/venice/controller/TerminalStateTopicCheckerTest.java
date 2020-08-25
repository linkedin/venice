package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.testng.Assert.*;

public class TerminalStateTopicCheckerTest {
  @Test
  public void testTerminalStateTopicChecker() {
    Properties properties = new Properties();
    properties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    properties.setProperty(TERMINAL_STATE_TOPIC_CHECK_DELAY_MS, String.valueOf(1000L));

    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1, 1, 1, 1, 100000, false, false, properties);
    VeniceControllerWrapper parentController = ServiceFactory.getVeniceParentController(venice.getClusterName(),
        ServiceFactory.getZkServer().getAddress(), venice.getKafka(),
        new VeniceControllerWrapper[]{venice.getMasterVeniceController()}, new VeniceProperties(properties), false);
    ControllerClient parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    String storeName = TestUtils.getUniqueString("testStore");
    assertFalse(parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"").isError(),
        "Failed to create test store");
    // Empty push without checking its push status
    VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test-push", 1000);
    assertFalse(response.isError(), "Failed to perform empty push on test store");
    // The empty push should eventually complete and have its version topic truncated by job status polling invoked by
    // the TerminalStateTopicCheckerForParentController.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true,
       () -> assertTrue(parentController.getVeniceAdmin().isTopicTruncated(response.getKafkaTopic())));

    parentControllerClient.close();
    parentController.close();
    venice.close();
  }
}
