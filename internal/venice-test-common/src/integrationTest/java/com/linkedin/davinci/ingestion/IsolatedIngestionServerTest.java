package com.linkedin.davinci.ingestion;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.INGESTION_ISOLATION_CONFIG_PREFIX;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.buildAndSaveConfigsForForkedIngestionProcess;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.executeShellCommand;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IsolatedIngestionServerTest {
  private static final int TIMEOUT_MS = 1 * Time.MS_PER_MINUTE;
  private ZkServerWrapper zkServerWrapper;

  @BeforeClass
  public void setUp() {
    zkServerWrapper = ServiceFactory.getZkServer();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(zkServerWrapper);
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testShutdownAfterHeartbeatTimeout() {
    int servicePort = TestUtils.getFreePort();
    VeniceConfigLoader configLoader = getConfigLoader(servicePort);
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(configLoader)) {
      Process isolatedIngestionService = client.startForkedIngestionProcess(configLoader);
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> Assert.assertTrue(client.sendHeartbeatRequest()));
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> Assert.assertFalse(isolatedIngestionService.isAlive()));
    }
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testReleaseTargetPortBinding() {
    int servicePort = TestUtils.getFreePort();
    VeniceConfigLoader configLoader = getConfigLoader(servicePort);
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(configLoader)) {
      // Make sure the process is forked successfully.
      ForkedJavaProcess isolatedIngestionService = (ForkedJavaProcess) client.startForkedIngestionProcess(configLoader);
      Assert.assertTrue(isolatedIngestionService.isAlive());
      // Make sure we have exactly 1 forked process.
      int matchIsolatedIngestionProcessCount = 0;
      String cmd = "lsof -t -i :" + servicePort;
      // Both RHEL & CentOS require an abs path of lsof. However, this path is not the same on Ubuntu. Also, the abs
      // path is not required on ubuntu. So we first try with the abs path and if it returns an empty string, that
      // likely means that the command did not succeed and hence we try it again without the abs path.
      String processIds = executeShellCommand("/usr/sbin/" + cmd);
      if (processIds.isEmpty()) {
        processIds = executeShellCommand(cmd);
      }
      for (String processId: processIds.split("\n")) {
        if (!processId.equals("")) {
          int pid = Integer.parseInt(processId);
          String fullProcessName = executeShellCommand("ps -p " + pid + " -o command");
          if (fullProcessName.contains(IsolatedIngestionServer.class.getName())) {
            matchIsolatedIngestionProcessCount += 1;
            Assert.assertEquals(pid, isolatedIngestionService.pid());
          }
        }
      }
      Assert.assertEquals(matchIsolatedIngestionProcessCount, 1);

      // Make sure forked process is killed.
      IsolatedIngestionUtils.releaseTargetPortBinding(servicePort);
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertEquals(executeShellCommand("/usr/sbin/" + cmd), "");
        Assert.assertEquals(executeShellCommand(cmd), "");
      });
    }
  }

  @Test
  public void testCustomizedConfigs() throws Exception {
    String testRegion = "ei-ltx1";
    // Set app region system property.
    System.setProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION, testRegion);
    VeniceConfigLoader configLoader = getConfigLoader(TestUtils.getFreePort());
    // Build and save config to file.
    String configFilePath = buildAndSaveConfigsForForkedIngestionProcess(configLoader);
    // Load config from saved config file.
    VeniceProperties loadedConfig = Utils.parseProperties(configFilePath);
    Assert.assertEquals(loadedConfig.getInt(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS), 10);
    // Venice Cluster config built on top of this config should contains expected region name passed from main process.
    Assert.assertEquals(RegionUtils.getLocalRegionName(loadedConfig, false), testRegion);
  }

  private VeniceConfigLoader getConfigLoader(int servicePort) {
    String dummyKafkaUrl = "localhost:1234";
    VeniceProperties properties = new PropertyBuilder().put(CLUSTER_NAME, Utils.getUniqueString())
        .put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress())
        .put(KAFKA_BOOTSTRAP_SERVERS, dummyKafkaUrl)
        .put(D2_ZK_HOSTS_ADDRESS, zkServerWrapper.getAddress())
        .put(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 100)
        .put(SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS, 10)
        .put(INGESTION_ISOLATION_CONFIG_PREFIX + "." + SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 10)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .build();
    return new VeniceConfigLoader(properties, properties);
  }
}
