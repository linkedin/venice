package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * A {@link DataWriterSparkJob} variant for the snapshot-at-T merge: its input DataFrame is the distributed cogroup
 * of the batch Avro input and every upstream region's real-time topics (read up to the cutoff), merged per key via
 * {@link SnapshotAtTSparkMerge}. Everything downstream of the input frame -- partitioning, chunking, RMD, producing
 * to the version topic, Start/End-Of-Push -- is the unchanged data-writer pipeline, so the merge is memory-bounded
 * per key-group instead of loading the whole batch + all RT into one process.
 *
 * <p>It is constructed reflectively by the normal data-writer launch path, so it reads all of its context (the
 * broadcast schema bundle, region brokers, cutoff, ...) from {@link PushJobSetting}, populated on the driver by
 * {@code VenicePushJob.maybeSetupSnapshotAtTDistributedMerge}.
 */
public class SnapshotAtTDataWriterSparkJob extends DataWriterSparkJob {
  @Override
  protected Dataset<Row> getUserInputDataFrame() {
    PushJobSetting setting = getPushJobSetting();
    StoreInfo storeInfo = setting.storeResponse.getStore();
    SnapshotAtTSchemaBundle bundle = setting.snapshotAtTSchemaBundle;
    int rmdProtocolVersion =
        bundle.getRmdVersionId() > 0 ? bundle.getRmdVersionId() : RmdSchemaGenerator.getLatestVersion();
    long cutoffMs = setting.snapshotAtTCutoffEpochSeconds > 0 ? setting.snapshotAtTCutoffEpochSeconds * 1000 : 0L;

    // The batch (key, value, rmd) frame from the Avro input, exactly as a normal Spark push reads it.
    Dataset<Row> batch = super.getUserInputDataFrame();
    SparkSession spark = getSparkSession();
    VeniceProperties props = getJobProperties();
    // Plan RT splits on the driver (queries brokers for partition ranges), then read them distributed.
    List<SnapshotAtTRtSplit> splits = new SnapshotAtTRtSplitPlanner()
        .plan(setting.snapshotAtTRtRegionBrokers, VenicePushJob.snapshotAtTRtTopicNames(storeInfo), props);
    Dataset<Row> rt = SnapshotAtTSparkMerge.readRtDataFrame(spark, splits, props, cutoffMs);
    Broadcast<SnapshotAtTSchemaBundle> broadcastBundle =
        JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(bundle);
    return SnapshotAtTSparkMerge.merge(
        batch,
        rt,
        setting.valueSchemaId,
        broadcastBundle,
        rmdProtocolVersion,
        setting.isStoreWriteComputeEnabled);
  }
}
