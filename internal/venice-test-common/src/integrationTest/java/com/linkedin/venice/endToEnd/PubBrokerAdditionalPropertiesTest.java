package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import org.testng.annotations.Test;


public class PubBrokerAdditionalPropertiesTest {
  VeniceClusterWrapper veniceClusterWrapper;

  @Test
  public void testPubBrokerAdditionalProperties() {
    veniceClusterWrapper = ServiceFactory.getVeniceCluster();
    System.out.println(veniceClusterWrapper);
  }

  @Test
  public void testPubBrokerAdditionalProperties2() {
    VeniceMultiClusterWrapper veniceMultiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(
        new VeniceMultiClusterCreateOptions.Builder(2).numberOfControllers(1).numberOfServers(1).build());
    System.out.println(veniceMultiClusterWrapper);
  }
}
