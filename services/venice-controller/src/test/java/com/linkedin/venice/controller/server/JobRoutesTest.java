package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.Utils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JobRoutesTest {
  private static final Logger LOGGER = LogManager.getLogger(JobRoutesTest.class);

  @Test
  public void testPopulateJobStatus() {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED)).when(mockAdmin)
        .getOffLinePushStatus(anyString(), anyString(), any());

    TopicManager mockTopicManager = mock(TopicManager.class);
    // 3 partitions, with latest offsets 100, 110, and 120
    Int2LongMap map = new Int2LongOpenHashMap();
    map.put(0, 100L);
    map.put(1, 110L);
    map.put(2, 120L);
    doReturn(map).when(mockTopicManager).getTopicLatestOffsets(anyString());
    doReturn(mockTopicManager).when(mockAdmin).getTopicManager();

    doReturn(2).when(mockAdmin).getReplicationFactor(anyString(), anyString());
    Map<String, Long> jobProgress = new HashMap<>();
    List<String> clusters = Arrays.asList("cluster1", "cluster2", "cluster3");
    for (String cluster: clusters) {
      for (int partition = 0; partition < mockTopicManager.getTopicLatestOffsets("").size(); partition++) {
        for (int replica = 0; replica < mockAdmin.getReplicationFactor("", ""); replica++) {
          String worker = cluster + "_p" + partition + "-r" + replica;
          jobProgress.put(worker, mockTopicManager.getTopicLatestOffsets("").get(partition)); // all workers complete
        }
      }
    }
    doReturn(jobProgress).when(mockAdmin).getOfflinePushProgress(anyString(), anyString());
    doReturn(clusters.size()).when(mockAdmin).getDatacenterCount(anyString());

    String cluster = Utils.getUniqueString("cluster");
    String store = Utils.getUniqueString("store");
    int version = 5;
    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty());
    JobStatusQueryResponse response = jobRoutes.populateJobStatus(cluster, store, version, mockAdmin, Optional.empty());

    long available = response.getMessagesAvailable();
    long consumed = response.getMessagesConsumed();

    Map<String, String> extraInfo = response.getExtraInfo();
    LOGGER.info("extraInfo: {}", extraInfo);
    Assert.assertNotNull(extraInfo);

    Map<String, String> extraDetails = response.getExtraDetails();
    LOGGER.info("extraDetails: {}", extraDetails);
    Assert.assertNotNull(extraDetails);

    Assert.assertTrue(
        consumed <= available,
        "Messages consumed: " + consumed + " must be less than or equal to available messages: " + available);
  }
}
