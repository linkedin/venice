package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import java.util.Properties;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


@Test(singleThreaded = true)
public class TestBatchForRocksDB extends TestBatch {

  @Override
  public VeniceClusterWrapper initializeVeniceCluster() {
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1, 0, 1);
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    veniceClusterWrapper.addVeniceServer(serverProperties);

    return veniceClusterWrapper;
  }
}
