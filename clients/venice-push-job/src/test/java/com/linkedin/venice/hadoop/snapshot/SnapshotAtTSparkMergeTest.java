package com.linkedin.venice.hadoop.snapshot;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.spark.SparkConstants;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link SnapshotAtTSparkMerge} on a local SparkSession: co-grouping a batch (key,value,rmd) frame
 * with an RT frame and reducing per key must produce the same converged result as the single-process executor --
 * RT wins over the batch base where it exists, batch-only keys keep their batch value -- proving the distributed
 * merge is correct end to end through Spark.
 */
public class SnapshotAtTSparkMergeTest {
  private static final String STRING_SCHEMA = "\"string\"";
  private static final String STORE = "snapshot_spark_merge_test";

  private SparkSession spark;
  private VeniceAvroKafkaSerializer serializer;
  private int rmdVersion;
  private SnapshotAtTSchemaBundle bundle;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    spark = SparkSession.builder().appName("snapshot-at-t-merge-test").master("local[2]").getOrCreate();
    serializer = new VeniceAvroKafkaSerializer(STRING_SCHEMA);
    Schema valueSchema = new Schema.Parser().parse(STRING_SCHEMA);
    rmdVersion = RmdSchemaGenerator.getLatestVersion();
    String rmdSchemaStr = RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersion).toString();

    Map<Integer, String> valueSchemas = new HashMap<>();
    valueSchemas.put(1, STRING_SCHEMA);
    Map<Long, String> rmdSchemas = new HashMap<>();
    rmdSchemas.put(SnapshotAtTSchemaRepository.key(1, rmdVersion), rmdSchemaStr);
    bundle = SnapshotAtTSchemaBundle.of(STORE, valueSchemas, rmdSchemas, Collections.emptyMap(), rmdVersion);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCogroupMergeRtWinsOverBatch() {
    // Batch keys 1, 2, 3.
    List<Row> batchRows = Arrays.asList(
        RowFactory.create(serialize("1"), serialize("batch_1"), null),
        RowFactory.create(serialize("2"), serialize("batch_2"), null),
        RowFactory.create(serialize("3"), serialize("batch_3"), null));
    Dataset<Row> batchDf = spark.createDataFrame(batchRows, SparkConstants.DEFAULT_SCHEMA);

    // RT: key 1 PUT rt_1 @1000 colo 0, key 3 PUT rt_3 @2000 colo 1 (key 2 has no RT).
    List<Row> rtRows = Arrays.asList(
        SnapshotAtTSparkMerge.rtRecordToRow(put("1", "rt_1", 1000L, 0)),
        SnapshotAtTSparkMerge.rtRecordToRow(put("3", "rt_3", 2000L, 1)));
    Dataset<Row> rtDf = spark.createDataFrame(rtRows, SnapshotAtTSparkMerge.RT_SCHEMA);

    Broadcast<SnapshotAtTSchemaBundle> broadcastBundle =
        JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(bundle);
    Dataset<Row> merged = SnapshotAtTSparkMerge.merge(batchDf, rtDf, 1, broadcastBundle, rmdVersion, false);

    Map<String, String> result = new HashMap<>();
    for (Row row: merged.collectAsList()) {
      String key = deserialize(row.getAs("key"));
      byte[] value = row.getAs("value");
      result.put(key, value == null ? null : deserialize(value));
    }

    assertEquals(result.size(), 3);
    assertEquals(result.get("1"), "rt_1"); // RT wins over the batch base
    assertEquals(result.get("2"), "batch_2"); // batch-only key keeps its batch value
    assertEquals(result.get("3"), "rt_3"); // RT wins
  }

  private byte[] serialize(String value) {
    return serializer.serialize(null, value);
  }

  private String deserialize(byte[] bytes) {
    return serializer.deserialize(null, bytes).toString();
  }

  private SnapshotAtTRtRecord put(String key, String value, long writeTimestamp, int coloId) {
    return new SnapshotAtTRtRecord(
        SnapshotAtTRtRecord.Op.PUT,
        ByteBuffer.wrap(serialize(key)),
        ByteBuffer.wrap(serialize(value)),
        1,
        -1,
        writeTimestamp,
        coloId);
  }
}
