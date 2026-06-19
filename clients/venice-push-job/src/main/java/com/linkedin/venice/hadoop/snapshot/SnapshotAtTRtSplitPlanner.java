package com.linkedin.venice.hadoop.snapshot;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitPlanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Plans the distributed RT-reading work for a snapshot-at-T merge across all upstream regions. For each region
 * (coloId &rarr; broker) and each RT topic to read (the regular RT, plus the separate RT for sepRT-enabled stores),
 * it reuses {@link PubSubSplitPlanner} to cut that topic into per-partition {@link PubSubPartitionSplit}s, then tags
 * each with its broker and coloId as a {@link SnapshotAtTRtSplit}. The union of all regions' splits is what the
 * distributed merge fans out over — so total RT-reading parallelism is {@code regions x topics x partitions x
 * splits-per-partition}, never a single-process drain.
 */
public class SnapshotAtTRtSplitPlanner {
  private final PubSubSplitPlanner splitPlanner;

  public SnapshotAtTRtSplitPlanner() {
    this(new PubSubSplitPlanner());
  }

  // Visible for testing: inject a stub planner so the multi-region fan-out can be verified without a live broker.
  SnapshotAtTRtSplitPlanner(PubSubSplitPlanner splitPlanner) {
    this.splitPlanner = splitPlanner;
  }

  /**
   * @param regionBrokers coloId &rarr; broker address for every upstream region to read
   * @param rtTopicNames the RT topic name(s) to read per region (regular RT, plus separate RT when enabled)
   * @param baseProps the job's pubsub client config (the per-region broker + topic are overlaid onto a copy of it)
   * @return one {@link SnapshotAtTRtSplit} per (region, topic, partition-split), tagged with broker + coloId
   */
  public List<SnapshotAtTRtSplit> plan(
      Map<Integer, String> regionBrokers,
      List<String> rtTopicNames,
      VeniceProperties baseProps) {
    List<SnapshotAtTRtSplit> splits = new ArrayList<>();
    for (Map.Entry<Integer, String> region: regionBrokers.entrySet()) {
      int coloId = region.getKey();
      String broker = region.getValue();
      for (String rtTopicName: rtTopicNames) {
        VeniceProperties regionProps = withSource(baseProps, broker, rtTopicName);
        for (PubSubPartitionSplit split: splitPlanner.plan(regionProps)) {
          splits.add(new SnapshotAtTRtSplit(split, broker, coloId));
        }
      }
    }
    return splits;
  }

  /** A copy of {@code baseProps} with the repush source broker + input topic pointed at one region's RT topic. */
  private static VeniceProperties withSource(VeniceProperties baseProps, String broker, String rtTopicName) {
    Properties properties = baseProps.toProperties();
    properties.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, broker);
    properties.setProperty(KAFKA_INPUT_TOPIC, rtTopicName);
    return new VeniceProperties(properties);
  }
}
