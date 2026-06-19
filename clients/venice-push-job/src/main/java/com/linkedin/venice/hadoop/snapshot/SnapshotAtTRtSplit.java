package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.Serializable;
import java.util.Objects;


/**
 * A unit of distributed RT-reading work for the snapshot-at-T merge: one {@link PubSubPartitionSplit} (a bounded
 * range of one RT topic-partition) plus the two things the shared split lacks for multi-region reads — the
 * {@code broker} the partition lives on and the {@code coloId} of that region (for cross-region Active-Active
 * conflict resolution). One split is processed by one distributed task.
 */
public class SnapshotAtTRtSplit implements Serializable {
  private static final long serialVersionUID = 1L;

  private final PubSubPartitionSplit split;
  private final String broker;
  private final int coloId;

  public SnapshotAtTRtSplit(PubSubPartitionSplit split, String broker, int coloId) {
    this.split = Objects.requireNonNull(split, "split");
    this.broker = Objects.requireNonNull(broker, "broker");
    this.coloId = coloId;
  }

  public PubSubPartitionSplit getSplit() {
    return split;
  }

  public String getBroker() {
    return broker;
  }

  public int getColoId() {
    return coloId;
  }

  @Override
  public String toString() {
    return "SnapshotAtTRtSplit{colo=" + coloId + ", broker=" + broker + ", split=" + split + "}";
  }
}
