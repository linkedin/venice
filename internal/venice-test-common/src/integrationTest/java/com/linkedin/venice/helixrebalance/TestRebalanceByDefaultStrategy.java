package com.linkedin.venice.helixrebalance;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRebalanceByDefaultStrategy {
  private static final Logger LOGGER = LogManager.getLogger(TestRebalanceByDefaultStrategy.class);
  private static final long TIMEOUT_MS = 30000l;
  private static final long UPGRADE_TIME_MS = 1000l;
  private static final long RETRY_TIME_MS = 500l;
  private static final long RETRY_REMOVE_TIMEOUT_MS = 5000l;

  private static final int TEST_TIMES = 1; // Could set up to 100 to run this test multiple times.

  private VeniceClusterWrapper cluster;
  private int numberOfController = 1;
  private int numberOfRouter = 0;
  private int numberOfServer = 5;
  private int partitionNumber = 2;
  private int replicationFactor = 3;
  private int partitionSize = 256;

  private String topicName;

  @BeforeClass
  public void setUp() {
    cluster = ServiceFactory.getVeniceCluster(
        numberOfController,
        numberOfServer,
        numberOfRouter,
        replicationFactor,
        partitionSize,
        false,
        false);
    String storeName = Utils.getUniqueString("testRollingUpgrade");
    long storageQuota = (long) partitionSize * partitionNumber;
    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    VersionCreationResponse response = cluster.getNewVersion(storeName);

    topicName = response.getKafkaTopic();

    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put("test", "test", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    TestUtils.waitForNonDeterministicCompletion(
        TIMEOUT_MS,
        TimeUnit.MILLISECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));
  }

  @AfterClass
  public void cleanUp() {
    cluster.close();
  }

  @Test(invocationCount = TEST_TIMES, skipFailedInvocations = false, timeOut = 60 * Time.MS_PER_SECOND)
  public void testRollingUpgrade() throws InterruptedException {
    String clusterName = cluster.getClusterName();
    Set<Integer> ports = new HashSet<>();
    cluster.getVeniceServers().forEach(wrapper -> ports.add(wrapper.getPort()));
    for (Integer port: ports) {
      String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
      TestUtils.waitForNonDeterministicCompletion(RETRY_REMOVE_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        try {
          if (cluster.getLeaderVeniceController()
              .getVeniceAdmin()
              .isInstanceRemovable(clusterName, instanceId, Collections.emptyList(), false)
              .isRemovable()) {
            cluster.stopVeniceServer(port);
            Thread.sleep(UPGRADE_TIME_MS);
            cluster.restartVeniceServer(port);
            return true;
          } else {
            Thread.sleep(RETRY_TIME_MS);
            return false;
          }
        } catch (InterruptedException e) {
          throw new VeniceException("Can not stop server on port:" + port, e);
        }
      });
    }

    // Ensure each partition has 3 online replica
    TestUtils.waitForNonDeterministicCompletion(TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
      List<Replica> replicas = cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName);
      StringBuilder sb = new StringBuilder();
      boolean isAllOnline = true;
      for (Replica replica: replicas) {
        if (replica.getStatus().equals(HelixState.ERROR) || replica.getStatus().equals(HelixState.OFFLINE)) {
          sb.append(replica.getInstance().getNodeId());
          sb.append(":");
          sb.append(replica.getPartitionId());
          sb.append(":");
          sb.append(replica.getStatus());
          sb.append("###");
          isAllOnline = false;
        }
      }
      LOGGER.info("Replica number:{}, non-online replicas:{}", replicas.size(), sb.toString());
      return (replicas.size() == partitionNumber * replicationFactor) && isAllOnline;
    });
  }
}
