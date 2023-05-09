package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.Utils;
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
        .getOffLinePushStatus(anyString(), anyString(), any(), any());

    doReturn(2).when(mockAdmin).getReplicationFactor(anyString(), anyString());

    String cluster = Utils.getUniqueString("cluster");
    String store = Utils.getUniqueString("store");
    int version = 5;
    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty(), Optional.empty(), Optional.empty());
    JobStatusQueryResponse response =
        jobRoutes.populateJobStatus(cluster, store, version, mockAdmin, Optional.empty(), null);

    Map<String, String> extraInfo = response.getExtraInfo();
    LOGGER.info("extraInfo: {}", extraInfo);
    Assert.assertNotNull(extraInfo);

    Map<String, String> extraDetails = response.getExtraDetails();
    LOGGER.info("extraDetails: {}", extraDetails);
    Assert.assertNotNull(extraDetails);
  }
}
