package com.linkedin.venice.spark.input.pubsub;

import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.connector.read.InputPartition;


/**
 * Thin Spark adapter. Serializable wrapper around PubSubPartitionSplit.
 */
public class SparkPubSubInputPartition implements InputPartition, Serializable {
  private static final long serialVersionUID = 1L;

  private final PubSubPartitionSplit split;

  public SparkPubSubInputPartition(PubSubPartitionSplit split) {
    this.split = Objects.requireNonNull(split, "PubSubPartitionSplit cannot be null");
  }

  public PubSubPartitionSplit getPubSubPartitionSplit() {
    return split;
  }

  @Override
  public String toString() {
    return "SparkPubSubInputPartition{" + split + "}";
  }
}
