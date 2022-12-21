package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;


public class PartialUpdateWithSingleReplicaTest extends PartialUpdateTest {
  @Override
  protected VeniceTwoLayerMultiColoMultiClusterWrapper getMultiColoMultiClusterWrapper(
      Properties serverProperties,
      Properties controllerProps) {
    return ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(new Properties(controllerProps)),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
  }
}
