package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestBatchForBDB extends TestBatch {
  @Override
  public VeniceClusterWrapper initializeVeniceCluster() {
    return ServiceFactory.getVeniceCluster();
  }
}
