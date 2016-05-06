package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixCachedMetadataRepository;
import com.linkedin.venice.helix.HelixJobRepository;
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
    HelixCachedMetadataRepository metaRepo = Mockito.mock(HelixCachedMetadataRepository.class);
    HelixJobRepository jobRepo = Mockito.mock(HelixJobRepository.class);
    doReturn(new ArrayList<Job>()).when(jobRepo).getRunningJobOfTopic(anyString());

    VeniceJobManager testManager = new VeniceJobManager(0, jobRepo, metaRepo);
    Assert.assertEquals(testManager.getOfflineJobStatus("atopic"), ExecutionStatus.NOT_CREATED,
        "VeniceJobManager must return NOT_CREATED on missing topic");
  }
}
