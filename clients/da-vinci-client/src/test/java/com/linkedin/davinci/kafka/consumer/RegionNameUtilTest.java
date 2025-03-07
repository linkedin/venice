package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.utils.PubSubHelper;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.testng.annotations.Test;


public class RegionNameUtilTest {
  @Test
  public void testGetRegionName() {
    Int2ObjectMap<String> regionIdToNameMap = new Int2ObjectOpenHashMap<>();
    regionIdToNameMap.put(0, "region0");
    regionIdToNameMap.put(1, "region1");
    regionIdToNameMap.put(2, "region2");

    PubSubHelper.MutableDefaultPubSubMessage message = PubSubHelper.getDummyPubSubMessage(false);
    message.getValue().leaderMetadataFooter = new LeaderMetadata();

    message.getValue().leaderMetadataFooter.upstreamKafkaClusterId = 0;
    assertEquals(RegionNameUtil.getRegionName(message, regionIdToNameMap), "region0");

    message.getValue().leaderMetadataFooter.upstreamKafkaClusterId = 1;
    assertEquals(RegionNameUtil.getRegionName(message, regionIdToNameMap), "region1");

    message.getValue().leaderMetadataFooter.upstreamKafkaClusterId = -1;
    assertEquals(RegionNameUtil.getRegionName(message, regionIdToNameMap), "");

    assertEquals(RegionNameUtil.getRegionName(null, regionIdToNameMap), "");
    assertEquals(RegionNameUtil.getRegionName(message, null), "");
  }
}
