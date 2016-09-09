package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixJobRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import java.util.ArrayList;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;


/**
 * VeniceJobManager tests that don't require zookeeper to be running
 */
public class UnitTestVeniceJobManager {

  @Test
  public void notCreatedStatusOnMissingJob(){
    HelixReadWriteStoreRepository metaRepo = Mockito.mock(HelixReadWriteStoreRepository.class);
    HelixJobRepository jobRepo = Mockito.mock(HelixJobRepository.class);
    HelixRoutingDataRepository routingRepo = Mockito.mock(HelixRoutingDataRepository.class);
    doReturn(new ArrayList<Job>()).when(jobRepo).getRunningJobOfTopic(anyString());

    VeniceJobManager testManager = new VeniceJobManager("cluster" , 0, jobRepo, metaRepo, routingRepo, 15000);
    Assert.assertEquals(testManager.getOfflineJobStatus("atopic"), ExecutionStatus.NOT_CREATED,
        "VeniceJobManager must return NOT_CREATED on missing topic");
  }
}
