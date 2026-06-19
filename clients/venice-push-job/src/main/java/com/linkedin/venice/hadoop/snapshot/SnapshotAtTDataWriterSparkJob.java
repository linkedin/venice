package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * A {@link DataWriterSparkJob} variant for the snapshot-at-T merge: its input DataFrame is the distributed cogroup
 * of the batch Avro input and every upstream region's real-time topics (read up to the cutoff), merged per key via
 * {@link SnapshotAtTSparkMerge}. Everything downstream of the input frame — partitioning, chunking, RMD, producing
 * to the version topic — is the unchanged data-writer pipeline, so the merge is memory-bounded per key-group
 * instead of loading the whole batch + all RT into one process.
 *
 * <p>The snapshot context (schemas, region brokers, cutoff, ...) is supplied via {@link #setSnapshotAtTContext}
 * before {@code runJob()} by the driver, which holds the controller client used to build the schema bundle.
 */
public class SnapshotAtTDataWriterSparkJob extends DataWriterSparkJob {
  private SnapshotAtTSchemaBundle schemaBundle;
  private Map<Integer, String> regionBrokers;
  private List<String> rtTopicNames;
  private long cutoffTimestampMs;
  private int batchValueSchemaId;
  private int rmdProtocolVersion;
  private boolean rmdUseFieldLevelTimestamp;

  public void setSnapshotAtTContext(
      SnapshotAtTSchemaBundle schemaBundle,
      Map<Integer, String> regionBrokers,
      List<String> rtTopicNames,
      long cutoffTimestampMs,
      int batchValueSchemaId,
      int rmdProtocolVersion,
      boolean rmdUseFieldLevelTimestamp) {
    this.schemaBundle = schemaBundle;
    this.regionBrokers = regionBrokers;
    this.rtTopicNames = rtTopicNames;
    this.cutoffTimestampMs = cutoffTimestampMs;
    this.batchValueSchemaId = batchValueSchemaId;
    this.rmdProtocolVersion = rmdProtocolVersion;
    this.rmdUseFieldLevelTimestamp = rmdUseFieldLevelTimestamp;
  }

  @Override
  protected Dataset<Row> getUserInputDataFrame() {
    // The batch (key, value, rmd) frame from the Avro input, exactly as a normal Spark push reads it.
    Dataset<Row> batch = super.getUserInputDataFrame();
    SparkSession spark = getSparkSession();
    VeniceProperties props = getJobProperties();
    // Plan RT splits on the driver (queries brokers for partition ranges), then read them distributed.
    List<SnapshotAtTRtSplit> splits = new SnapshotAtTRtSplitPlanner().plan(regionBrokers, rtTopicNames, props);
    Dataset<Row> rt = SnapshotAtTSparkMerge.readRtDataFrame(spark, splits, props, cutoffTimestampMs);
    Broadcast<SnapshotAtTSchemaBundle> broadcastBundle =
        JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(schemaBundle);
    return SnapshotAtTSparkMerge
        .merge(batch, rt, batchValueSchemaId, broadcastBundle, rmdProtocolVersion, rmdUseFieldLevelTimestamp);
  }
}
