package com.linkedin.venice.hadoop.snapshot;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRecordMerger.MergedRecord;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.CoGroupFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * The distributed snapshot-at-T merge: co-groups the batch {@code (key, value, rmd)} DataFrame with the real-time
 * (RT) DataFrame by key and reduces each key through {@link SnapshotAtTPushExecutor#mergeKey} (the same per-key
 * merge the single-process executor uses), emitting the merged {@code (key, value, rmd)} DataFrame. Because Spark
 * co-locates every value for a key on one task, memory is bounded per key-group — never the whole dataset.
 *
 * <p>The {@link SnapshotAtTRecordMerger} wraps a non-serializable conflict resolver, so it is rebuilt on each task
 * from a broadcast {@link SnapshotAtTSchemaBundle} rather than shipped.
 */
public final class SnapshotAtTSparkMerge {
  // RT record columns. The key column shares KEY_COLUMN_NAME so both sides of the cogroup group on the same field.
  static final int RT_KEY = 0;
  static final int RT_OP = 1;
  static final int RT_PAYLOAD = 2;
  static final int RT_VALUE_SCHEMA_ID = 3;
  static final int RT_UPDATE_PROTOCOL_VERSION = 4;
  static final int RT_WRITE_TIMESTAMP = 5;
  static final int RT_COLO_ID = 6;

  public static final StructType RT_SCHEMA = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, DataTypes.BinaryType, false, Metadata.empty()),
          new StructField("op", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("payload", DataTypes.BinaryType, true, Metadata.empty()),
          new StructField("valueSchemaId", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("updateProtocolVersion", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("writeTimestamp", DataTypes.LongType, false, Metadata.empty()),
          new StructField("coloId", DataTypes.IntegerType, false, Metadata.empty()) });

  private SnapshotAtTSparkMerge() {
  }

  /**
   * Co-group {@code batch} (key,value,rmd) with {@code rt} ({@link #RT_SCHEMA}) by key and merge each key, returning
   * the merged {@code (key,value,rmd)} DataFrame ({@link DEFAULT_SCHEMA}). A merged delete is emitted with a null
   * value and the RMD tombstone.
   *
   * @param batchValueSchemaId the value schema id the batch values are serialized with
   * @param bundle broadcast schemas used to rebuild the merger per task
   * @param rmdProtocolVersion the store's RMD protocol version
   * @param rmdUseFieldLevelTimestamp whether the store uses per-field RMD timestamps (write-compute)
   */
  public static Dataset<Row> merge(
      Dataset<Row> batch,
      Dataset<Row> rt,
      int batchValueSchemaId,
      Broadcast<SnapshotAtTSchemaBundle> bundle,
      int rmdProtocolVersion,
      boolean rmdUseFieldLevelTimestamp) {
    KeyValueGroupedDataset<byte[], Row> batchByKey =
        batch.groupByKey((MapFunction<Row, byte[]>) row -> row.getAs(KEY_COLUMN_NAME), Encoders.BINARY());
    KeyValueGroupedDataset<byte[], Row> rtByKey =
        rt.groupByKey((MapFunction<Row, byte[]>) row -> row.getAs(KEY_COLUMN_NAME), Encoders.BINARY());
    return batchByKey.cogroup(
        rtByKey,
        new MergeCoGroupFunction(bundle, batchValueSchemaId, rmdProtocolVersion, rmdUseFieldLevelTimestamp),
        RowEncoder.apply(DEFAULT_SCHEMA));
  }

  /**
   * Build the RT {@link #RT_SCHEMA} DataFrame by reading every {@link SnapshotAtTRtSplit} in parallel: one Spark
   * task per split reads only that split's bounded partition-range via {@link SnapshotAtTRtSplitReader}, so RT is
   * never drained into a single process. The split planning (which queries brokers) happens on the driver; this
   * distributes the reads.
   *
   * @param splits the region/topic/partition splits to read (from {@link SnapshotAtTRtSplitPlanner})
   * @param baseProps the job's pubsub client config (each split overlays its own broker)
   * @param cutoffTimestampMs include only RT records with write timestamp &le; this; {@code <= 0} means no bound
   */
  public static Dataset<Row> readRtDataFrame(
      SparkSession spark,
      List<SnapshotAtTRtSplit> splits,
      VeniceProperties baseProps,
      long cutoffTimestampMs) {
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    // Capture only serializable state (Properties, primitives, serializable splits) in the executor closure.
    Properties props = baseProps.toProperties();
    JavaRDD<Row> rtRows = sparkContext.parallelize(splits).flatMap((FlatMapFunction<SnapshotAtTRtSplit, Row>) split -> {
      List<SnapshotAtTRtRecord> records =
          new SnapshotAtTRtSplitReader().read(split, new VeniceProperties(props), cutoffTimestampMs);
      List<Row> rows = new ArrayList<>(records.size());
      for (SnapshotAtTRtRecord record: records) {
        rows.add(rtRecordToRow(record));
      }
      return rows.iterator();
    });
    return spark.createDataFrame(rtRows, RT_SCHEMA);
  }

  /** Convert one normalized RT record to an {@link #RT_SCHEMA} row (driver side, when building the RT DataFrame). */
  public static Row rtRecordToRow(SnapshotAtTRtRecord record) {
    return new GenericRowWithSchema(
        new Object[] { toByteArray(record.getKey()), record.getOp().ordinal(),
            record.getPayload() == null ? null : toByteArray(record.getPayload()), record.getValueSchemaId(),
            record.getUpdateProtocolVersion(), record.getWriteTimestamp(), record.getColoId() },
        RT_SCHEMA);
  }

  static SnapshotAtTRtRecord rowToRtRecord(Row row) {
    byte[] payload = row.isNullAt(RT_PAYLOAD) ? null : row.getAs(RT_PAYLOAD);
    return new SnapshotAtTRtRecord(
        SnapshotAtTRtRecord.Op.values()[row.getInt(RT_OP)],
        ByteBuffer.wrap(row.getAs(RT_KEY)),
        payload == null ? null : ByteBuffer.wrap(payload),
        row.getInt(RT_VALUE_SCHEMA_ID),
        row.getInt(RT_UPDATE_PROTOCOL_VERSION),
        row.getLong(RT_WRITE_TIMESTAMP),
        row.getInt(RT_COLO_ID));
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.rewind();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  /** Per-key reducer: rebuilds the merger from the broadcast bundle once per task, then folds via mergeKey. */
  private static final class MergeCoGroupFunction implements CoGroupFunction<byte[], Row, Row, Row> {
    private static final long serialVersionUID = 1L;

    private final Broadcast<SnapshotAtTSchemaBundle> bundle;
    private final int batchValueSchemaId;
    private final int rmdProtocolVersion;
    private final boolean rmdUseFieldLevelTimestamp;
    private transient SnapshotAtTRecordMerger merger;

    MergeCoGroupFunction(
        Broadcast<SnapshotAtTSchemaBundle> bundle,
        int batchValueSchemaId,
        int rmdProtocolVersion,
        boolean rmdUseFieldLevelTimestamp) {
      this.bundle = bundle;
      this.batchValueSchemaId = batchValueSchemaId;
      this.rmdProtocolVersion = rmdProtocolVersion;
      this.rmdUseFieldLevelTimestamp = rmdUseFieldLevelTimestamp;
    }

    @Override
    public Iterator<Row> call(byte[] key, Iterator<Row> batchRows, Iterator<Row> rtRows) {
      ByteBuffer batchValue = null;
      if (batchRows.hasNext()) {
        byte[] value = batchRows.next().getAs(VALUE_COLUMN_NAME);
        batchValue = value == null ? null : ByteBuffer.wrap(value);
      }
      List<SnapshotAtTRtRecord> rtRecords = new ArrayList<>();
      while (rtRows.hasNext()) {
        rtRecords.add(rowToRtRecord(rtRows.next()));
      }
      if (batchValue == null && rtRecords.isEmpty()) {
        return Collections.emptyIterator();
      }
      MergedRecord merged = SnapshotAtTPushExecutor.mergeKey(batchValue, batchValueSchemaId, rtRecords, merger());
      byte[] valueBytes = merged.isDelete() ? null : toByteArray(merged.getValue());
      byte[] rmdBytes = toByteArray(merged.getRmd());
      Row out = new GenericRowWithSchema(new Object[] { key, valueBytes, rmdBytes }, DEFAULT_SCHEMA);
      return Collections.singletonList(out).iterator();
    }

    private SnapshotAtTRecordMerger merger() {
      if (merger == null) {
        merger = bundle.value().buildMerger(rmdProtocolVersion, rmdUseFieldLevelTimestamp);
      }
      return merger;
    }
  }
}
