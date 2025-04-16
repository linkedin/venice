package com.linkedin.venice.helix;

import com.linkedin.venice.controller.ControllerInstanceTagRefresher;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for {@link ControllerInstanceTagRefresher}, which is responsible for ensuring that
 * expected instance tags are applied to a controller participant in Helix before it transitions to a live state.
 *
 * These tests verify:
 * 1. Instance is brand-new to the Helix cluster (Scale ups)
 * 2. Instance already exists in the cluster, and contains the same state as it expects (in-place deployments)
 * 3. Instance already exists in the cluster, but contains different state from what it expects (stale metadata after various operations)
 *
 * Simulates real-world scenarios such as Kubernetes restarts, where participants may come up with missing config,
 * and ensures the preConnect callback mechanism reliably restores expected cluster state.
 */
public class ControllerInstanceTagRefresherTest {
  private static final String CLUSTER_NAME = "test-controller-cluster";
  private static final String INSTANCE_TAG_1 = "venice-tag-1";
  private static final String INSTANCE_TAG_2 = "venice-tag-2";

  private ZkServerWrapper zkServerWrapper;
  private String zkAddress;
  private HelixAdmin admin;

  @BeforeClass
  public void setUp() throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();

    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(CLUSTER_NAME);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(CLUSTER_NAME).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(CLUSTER_NAME, LeaderStandbySMD.name, LeaderStandbySMD.build());
  }

  @AfterClass
  public void tearDown() {
    zkServerWrapper.close();
  }

  @Test
  public void testControllerInstanceTagRefresherAddsExpectedTags() throws Exception {
    int port = 50000 + (int) (System.currentTimeMillis() % 10000);
    String controllerName = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    List<String> expectedTags = Arrays.asList(INSTANCE_TAG_1, INSTANCE_TAG_2);

    // Create a Helix participant and register the callback
    SafeHelixManager tempManager =
        new SafeHelixManager(new ZKHelixManager(CLUSTER_NAME, controllerName, InstanceType.PARTICIPANT, zkAddress));
    tempManager.addPreConnectCallback(new ControllerInstanceTagRefresher(tempManager, expectedTags));

    // Connect will register the instance in ZK and trigger preConnect
    tempManager.connect();

    // Give Helix time to trigger the callback and update tags
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      ConfigAccessor configAccessor = tempManager.getConfigAccessor();
      InstanceConfig updatedConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, controllerName);
      List<String> actualTags = updatedConfig.getTags();
      Assert.assertTrue(actualTags.containsAll(expectedTags), "Instance tags not updated correctly: " + actualTags);
    });

    tempManager.disconnect();
  }

  @Test
  public void testControllerInstanceTagRefresherReappliesTagsOnReconnect() throws Exception {
    int port = 50000 + (int) (System.currentTimeMillis() % 10000);
    String controllerName = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    List<String> expectedTags = Arrays.asList(INSTANCE_TAG_1, INSTANCE_TAG_2);

    // Initial connect: create and register participant with callback
    SafeHelixManager firstManager =
        new SafeHelixManager(new ZKHelixManager(CLUSTER_NAME, controllerName, InstanceType.PARTICIPANT, zkAddress));
    firstManager.addPreConnectCallback(new ControllerInstanceTagRefresher(firstManager, expectedTags));
    firstManager.connect();

    // Verify tags are applied
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      ConfigAccessor configAccessor = firstManager.getConfigAccessor();
      InstanceConfig config = configAccessor.getInstanceConfig(CLUSTER_NAME, controllerName);
      Assert.assertTrue(config.getTags().containsAll(expectedTags), "Initial tag update failed");
    });

    // Simulate shutdown
    firstManager.disconnect();

    // Simulate manual tag removal (e.g., K8s restarted with a stale config)
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT).forCluster(CLUSTER_NAME)
            .forParticipant(controllerName)
            .build();

    // Remove tags by setting INSTANCE_TAGS to an empty string
    admin.setConfig(scope, Collections.singletonMap("INSTANCE_TAGS", ""));

    // Reconnect with same controller instance
    SafeHelixManager secondManager =
        new SafeHelixManager(new ZKHelixManager(CLUSTER_NAME, controllerName, InstanceType.PARTICIPANT, zkAddress));
    secondManager.addPreConnectCallback(new ControllerInstanceTagRefresher(secondManager, expectedTags));
    secondManager.connect();

    // Verify tags are re-applied
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      InstanceConfig updatedConfig = admin.getInstanceConfig(CLUSTER_NAME, controllerName);
      List<String> actualTags = updatedConfig.getTags();

      // Verify tags are re-applied after rejoin
      Assert.assertTrue(
          actualTags.containsAll(expectedTags),
          "Tags not re-applied after rejoin. Expected: " + expectedTags + ", Found: " + actualTags);
    });

    secondManager.disconnect();
  }
}
