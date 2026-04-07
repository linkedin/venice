package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.stats.dimensions.VeniceDimensionTestFixture;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class ConsumerPoolTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<ConsumerPoolType, String> expectedValues = CollectionUtils.<ConsumerPoolType, String>mapBuilder()
        .put(ConsumerPoolType.REGULAR_POOL, "regular_pool")
        .put(ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL, "current_version_aa_wc_leader_pool")
        .put(ConsumerPoolType.CURRENT_VERSION_SEP_RT_LEADER_POOL, "current_version_sep_rt_leader_pool")
        .put(ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL, "current_version_non_aa_wc_leader_pool")
        .put(ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL, "non_current_version_aa_wc_leader_pool")
        .put(ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL, "non_current_version_non_aa_wc_leader_pool")
        .build();
    new VeniceDimensionTestFixture<>(
        ConsumerPoolType.class,
        VeniceMetricsDimensions.VENICE_CONSUMER_POOL_TYPE,
        expectedValues).assertAll();
  }
}
