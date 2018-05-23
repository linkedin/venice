package com.linkedin.venice.controller.server;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class JobRoutesTest {

  private static final Logger LOGGER = Logger.getLogger(JobRoutesTest.class);

  @Test
  public void testPopulateJobStatus() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isMasterController(anyString());
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED)).when(mockAdmin).getOffLinePushStatus(anyString(), anyString());

    TopicManager mockTopicManager = mock(TopicManager.class);
    // 3 partitions, with latest offsets 100, 110, and 120
    doReturn(ImmutableMap.of(0, 100L, 1, 110L, 2, 120L)).when(mockTopicManager).getLatestOffsets(anyString());
    doReturn(mockTopicManager).when(mockAdmin).getTopicManager();

    doReturn(2).when(mockAdmin).getReplicationFactor(anyString(), anyString());
    Map<String, Long> jobProgress = new HashMap<>();
    List<String> clusters = Arrays.asList("cluster1", "cluster2", "cluster3");
    for (String cluster : clusters){
      for (int partition=0; partition < mockTopicManager.getLatestOffsets("").size(); partition ++){
        for (int replica=0; replica < mockAdmin.getReplicationFactor("",""); replica++){
          String worker = cluster + "_p" + partition+"-r"+replica;
          jobProgress.put(worker, mockTopicManager.getLatestOffsets("").get(partition)); // all workers complete
        }
      }
    }
    doReturn(jobProgress).when(mockAdmin).getOfflinePushProgress(anyString(), anyString());
    doReturn(clusters.size()).when(mockAdmin).getDatacenterCount(anyString());

    String cluster = TestUtils.getUniqueString("cluster");
    String store = TestUtils.getUniqueString("store");
    int version = 5;
    JobStatusQueryResponse response = JobRoutes.populateJobStatus(cluster, store, version, mockAdmin);

    Long available = response.getMessagesAvailable();
    Long consumed = response.getMessagesConsumed();

    Map<String, String> extraInfo = response.getExtraInfo();
    LOGGER.info("extraInfo: " + extraInfo);
    Assert.assertNotNull(extraInfo);

    Map<String, String> extraDetails = response.getExtraDetails();
    LOGGER.info("extraDetails: " + extraDetails);
    Assert.assertNotNull(extraDetails);


    Assert.assertTrue(consumed <= available, "Messages consumed: " + consumed + " must be less than or equal to available messages: " + available);

  }
}