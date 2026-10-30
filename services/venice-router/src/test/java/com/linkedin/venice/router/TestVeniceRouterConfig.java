package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY;
import static com.linkedin.venice.ConfigKeys.ROUTER_HELIX_GROUP_LATENCY_ADAPTIVE_BATCH_GET_THRESHOLD_MS;
import static com.linkedin.venice.ConfigKeys.ROUTER_HELIX_GROUP_SKEW_RAMP_EXPONENT;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.router.api.routing.helix.HelixGroupLatencyAdaptiveStrategy;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategyEnum;
import com.linkedin.venice.utils.BatchGetConfigUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.TreeMap;
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

  @Test
  public void helixGroupLatencyKnobsDefaultToStrategyConstants() {
    VeniceRouterConfig config = new VeniceRouterConfig(getPropertyBuilderWithBasicConfigsFilledIn().build());
    // Unset -> the group selection strategy stays the least-loaded default and the latency knobs fall back to the
    // strategy's own defaults, so enabling LATENCY_ADAPTIVE later needs no extra config.
    assertEquals(config.getHelixGroupSelectionStrategy(), HelixGroupSelectionStrategyEnum.LEAST_LOADED);
    // The per-bucket full-skew thresholds default to the long-tail-retry batch-get thresholds.
    assertEquals(
        config.getHelixGroupLatencyAdaptiveBatchGetThresholdMs(),
        config.getLongTailRetryForBatchGetThresholdMs());
    assertEquals(
        config.getHelixGroupSkewRampExponent(),
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_INTERPOLATION_EXPONENT);
    assertEquals(
        config.getHelixGroupEvenUntilFraction(),
        HelixGroupLatencyAdaptiveStrategy.DEFAULT_EVEN_UNTIL_FRACTION);
    assertEquals(config.getHelixGroupMinSampleCount(), HelixGroupLatencyAdaptiveStrategy.DEFAULT_MIN_SAMPLE_COUNT);
  }

  @Test
  public void helixGroupLatencyKnobsAreOverridable() {
    String batchGetThresholds = "1-10:5,11-:20";
    VeniceProperties props = getPropertyBuilderWithBasicConfigsFilledIn()
        .put(
            ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY,
            HelixGroupSelectionStrategyEnum.LATENCY_ADAPTIVE.name())
        .put(ROUTER_HELIX_GROUP_LATENCY_ADAPTIVE_BATCH_GET_THRESHOLD_MS, batchGetThresholds)
        .put(ROUTER_HELIX_GROUP_SKEW_RAMP_EXPONENT, 2.0)
        .build();
    VeniceRouterConfig config = new VeniceRouterConfig(props);
    assertEquals(config.getHelixGroupSelectionStrategy(), HelixGroupSelectionStrategyEnum.LATENCY_ADAPTIVE);
    assertEquals(
        config.getHelixGroupLatencyAdaptiveBatchGetThresholdMs(),
        new TreeMap<>(BatchGetConfigUtils.parseRetryThresholdForBatchGet(batchGetThresholds)));
    assertEquals(config.getHelixGroupSkewRampExponent(), 2.0);
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
