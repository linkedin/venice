package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;
import static com.linkedin.venice.ConfigKeys.*;


public class IsolatedIngestionServerTest {
  private ZkServerWrapper zkServerWrapper;

  @BeforeClass
  public void setup() {
    zkServerWrapper = ServiceFactory.getZkServer();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(zkServerWrapper);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testShutdownAfterHeartbeatTimeout() {
    int servicePort = Utils.getFreePort();
    VeniceConfigLoader configLoader = getConfigLoader(servicePort);
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(IsolatedIngestionUtils.getSSLFactoryForInterProcessCommunication(configLoader), servicePort)) {
      Process isolatedIngestionService = client.startForkedIngestionProcess(configLoader);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, false, () -> {
        Assert.assertTrue(client.sendHeartbeatRequest());
      });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, false, () -> {
        Assert.assertFalse(isolatedIngestionService.isAlive());
      });
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testReleaseTargetPortBinding() {
    int servicePort = Utils.getFreePort();
    VeniceConfigLoader configLoader = getConfigLoader(servicePort);
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(IsolatedIngestionUtils.getSSLFactoryForInterProcessCommunication(configLoader), servicePort)) {
      // Make sure the process is forked successfully.
      ForkedJavaProcess isolatedIngestionService = (ForkedJavaProcess) client.startForkedIngestionProcess(configLoader);
      Assert.assertTrue(isolatedIngestionService.isAlive());
      // Make sure we have exactly 1 forked process.
      int matchIsolatedIngestionProcessCount = 0;
      String processIds = executeShellCommand("/usr/sbin/lsof -t -i :" + servicePort);
      for (String processId : processIds.split("\n")) {
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
        Assert.assertEquals(executeShellCommand("/usr/sbin/lsof -t -i :" + servicePort), "");
      });
    }
  }

  @Test
  public void testCustomizedConfigs() throws Exception {
    VeniceConfigLoader configLoader = getConfigLoader(Utils.getFreePort());
    String configFilePath = buildAndSaveConfigsForForkedIngestionProcess(configLoader);

    VeniceProperties loadedConfig = Utils.parseProperties(configFilePath);
    Assert.assertEquals(loadedConfig.getInt(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS), 10);
  }

  private VeniceConfigLoader getConfigLoader(int servicePort) {
    VeniceProperties properties = new PropertyBuilder().put(CLUSTER_NAME, TestUtils.getUniqueString())
        .put(ZOOKEEPER_ADDRESS, zkServerWrapper.getAddress())
        .put(KAFKA_BOOTSTRAP_SERVERS, TestUtils.getUniqueString())
        .put(KAFKA_ZK_ADDRESS, TestUtils.getUniqueString())
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, zkServerWrapper.getAddress())
        .put(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 100)
        .put(SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS, 10 * Time.MS_PER_SECOND)
        .put(INGESTION_ISOLATION_CONFIG_PREFIX + "." + SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 10)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .build();
    return new VeniceConfigLoader(properties, properties);
  }
}
