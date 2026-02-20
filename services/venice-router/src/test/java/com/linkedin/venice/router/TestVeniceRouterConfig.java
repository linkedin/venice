package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import org.testng.annotations.Test;


public class TestVeniceRouterConfig {
  @Test
  public void basicConstruction() {
    VeniceProperties props = getPropertyBuilderWithBasicConfigsFilledIn().build();
    VeniceRouterConfig routerConfig = new VeniceRouterConfig(props);
    Map<String, String> clusterToD2Map = routerConfig.getClusterToD2Map();
    assertEquals(clusterToD2Map.size(), 1);
    assertEquals(clusterToD2Map.get("blah"), "blahD2");
  }

  private PropertyBuilder getPropertyBuilderWithBasicConfigsFilledIn() {
    return new PropertyBuilder().put(CLUSTER_NAME, "blah")
        .put(LISTENER_PORT, 1)
        .put(LISTENER_SSL_PORT, 2)
        .put(ZOOKEEPER_ADDRESS, "host:1234")
        .put(KAFKA_BOOTSTRAP_SERVERS, "host:2345")
        .put(CLUSTER_TO_D2, "blah:blahD2");
  }
}
