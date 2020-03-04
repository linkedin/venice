package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;

import org.testng.annotations.Test;

import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;


@Test(singleThreaded = true)
public class TestBatchForBDB extends TestBatch {
  @Override
  public VeniceClusterWrapper initializeVeniceCluster() {
    VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 1);
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.BDB);
    cluster.addVeniceServer(serverProperties);
    return cluster;
  }
}
