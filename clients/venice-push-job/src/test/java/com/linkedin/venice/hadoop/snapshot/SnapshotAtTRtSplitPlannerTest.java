package com.linkedin.venice.hadoop.snapshot;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitPlanner;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link SnapshotAtTRtSplitPlanner}: it must fan {@link PubSubSplitPlanner} out across every region
 * and every RT topic, tagging each resulting split with the right broker + coloId.
 */
public class SnapshotAtTRtSplitPlannerTest {
  @Test
  public void testPlanFansOutAcrossRegionsAndTopics() {
    // The injected planner returns one split per call, tagged with the topic it was asked to plan, so the fan-out
    // (region, topic) -> tagged splits can be asserted without a live broker.
    PubSubSplitPlanner planner = mock(PubSubSplitPlanner.class);
    when(planner.plan(any())).thenAnswer(invocation -> {
      VeniceProperties regionProps = invocation.getArgument(0);
      PubSubPartitionSplit split = mock(PubSubPartitionSplit.class);
      when(split.getTopicName()).thenReturn(regionProps.getString(KAFKA_INPUT_TOPIC));
      return Collections.singletonList(split);
    });

    Map<Integer, String> regionBrokers = new LinkedHashMap<>();
    regionBrokers.put(0, "broker0:9092");
    regionBrokers.put(1, "broker1:9092");
    List<String> rtTopicNames = Arrays.asList("store_rt", "store_rt_sep");

    List<SnapshotAtTRtSplit> splits =
        new SnapshotAtTRtSplitPlanner(planner).plan(regionBrokers, rtTopicNames, VeniceProperties.empty());

    // 2 regions x 2 topics x 1 split each, each tagged with its own broker + coloId + topic.
    assertEquals(splits.size(), 4);
    assertTrue(hasSplit(splits, 0, "broker0:9092", "store_rt"));
    assertTrue(hasSplit(splits, 0, "broker0:9092", "store_rt_sep"));
    assertTrue(hasSplit(splits, 1, "broker1:9092", "store_rt"));
    assertTrue(hasSplit(splits, 1, "broker1:9092", "store_rt_sep"));
  }

  private static boolean hasSplit(List<SnapshotAtTRtSplit> splits, int coloId, String broker, String topicName) {
    return splits.stream()
        .anyMatch(
            s -> s.getColoId() == coloId && s.getBroker().equals(broker)
                && s.getSplit().getTopicName().equals(topicName));
  }
}
