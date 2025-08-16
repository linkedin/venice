package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitPlanner;
import java.util.List;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;


public class SparkPubSubInputFormat implements Scan, Batch {
  private static final Logger LOGGER = LogManager.getLogger(SparkPubSubInputFormat.class);

  private final VeniceProperties jobConfig;
  private final Supplier<PubSubSplitPlanner> plannerSupplier;

  public SparkPubSubInputFormat(VeniceProperties jobConfig) {
    this(jobConfig, PubSubSplitPlanner::new);
  }

  @VisibleForTesting
  SparkPubSubInputFormat(VeniceProperties jobConfig, Supplier<PubSubSplitPlanner> plannerSupplier) {
    this.jobConfig = jobConfig;
    this.plannerSupplier = plannerSupplier != null ? plannerSupplier : PubSubSplitPlanner::new;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    PubSubSplitPlanner planner = plannerSupplier.get();
    List<PubSubPartitionSplit> planned = planner.plan(jobConfig);
    InputPartition[] partitions = new InputPartition[planned.size()];

    int index = 0;
    for (PubSubPartitionSplit pubSubSplit: planned) {
      SparkPubSubInputPartition partition = new SparkPubSubInputPartition(pubSubSplit);
      partitions[index++] = partition;
      LOGGER.info("Created Spark input partition: {}", partition);
    }

    LOGGER.info(
        "Created {} Spark input partitions for topic: {}",
        partitions.length,
        jobConfig.getString(KAFKA_INPUT_TOPIC));
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SparkPubSubPartitionReaderFactory(jobConfig);
  }

  @Override
  public StructType readSchema() {
    return null;
  }
}
