package com.linkedin.venice.duckdb;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.SINGLE_FIELD_RECORD_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.TWO_FIELDS_RECORD_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.DaVinciRecordTransformerUtility;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DuckDBDaVinciRecordTransformerTest {
  static final int storeVersion = 1;
  static final int partitionId = 0;
  static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  static final String storeName = "test_store";
  private final Set<String> columnsToProject = Collections.emptySet();

  @Test
  public void testRecordTransformer() {
    String tempDir = Utils.getTempDataDirectory().getAbsolutePath();

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    try (DuckDBDaVinciRecordTransformer recordTransformer = new DuckDBDaVinciRecordTransformer(
        storeName,
        storeVersion,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        tempDir,
        columnsToProject)) {
      assertTrue(recordTransformer.useUniformInputValueSchema());

      Schema keySchema = recordTransformer.getKeySchema();
      assertEquals(keySchema.getType(), Schema.Type.RECORD);

      Schema outputValueSchema = recordTransformer.getOutputValueSchema();
      assertEquals(outputValueSchema.getType(), Schema.Type.RECORD);

      recordTransformer.onStartVersionIngestion(1, true);

      GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
      keyRecord.put("key", "key");
      Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

      GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
      valueRecord.put("firstName", "Duck");
      valueRecord.put("lastName", "Goose");
      Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord);

      DaVinciRecordTransformerResult<GenericRecord> transformerResult =
          recordTransformer.transform(lazyKey, lazyValue, partitionId, null);
      recordTransformer.processPut(lazyKey, lazyValue, partitionId, null);
      assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.UNCHANGED);
      // Result will be empty when it's UNCHANGED
      assertNull(transformerResult.getValue());
      assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue, partitionId, null));

      recordTransformer.processDelete(lazyKey, partitionId, null);

      assertFalse(recordTransformer.getStoreRecordsInDaVinci());

      int classHash = recordTransformer.getClassHash();

      DaVinciRecordTransformerUtility<GenericRecord, GenericRecord> recordTransformerUtility =
          recordTransformer.getRecordTransformerUtility();
      OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);

      assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));

      offsetRecord.setRecordTransformerClassHash(classHash);

      assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));
    }
  }

  @Test
  public void testVersionSwap() throws SQLException {
    String tempDir = Utils.getTempDataDirectory().getAbsolutePath();

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    DuckDBDaVinciRecordTransformer recordTransformer_v1 = new DuckDBDaVinciRecordTransformer(
        storeName,
        1,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        tempDir,
        columnsToProject);
    DuckDBDaVinciRecordTransformer recordTransformer_v2 = new DuckDBDaVinciRecordTransformer(
        storeName,
        2,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        tempDir,
        columnsToProject);

    String duckDBUrl = recordTransformer_v1.getDuckDBUrl();

    recordTransformer_v1.onStartVersionIngestion(1, true);
    recordTransformer_v2.onStartVersionIngestion(1, false);

    GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecord.put("key", "key");
    Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

    GenericRecord valueRecord_v1 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord_v1.put("firstName", "Duck");
    valueRecord_v1.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord_v1);
    recordTransformer_v1.processPut(lazyKey, lazyValue, partitionId, null);

    GenericRecord valueRecord_v2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord_v2.put("firstName", "Goose");
    valueRecord_v2.put("lastName", "Duck");
    lazyValue = Lazy.of(() -> valueRecord_v2);
    recordTransformer_v2.processPut(lazyKey, lazyValue, partitionId, null);

    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      assertDataset1(stmt, storeName);

      // Swap here
      recordTransformer_v1.onEndVersionIngestion(2);

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + storeName)) {
        assertTrue(rs.next(), "There should be a first row!");
        assertEquals(rs.getString("firstName"), "Goose");
        assertEquals(rs.getString("lastName"), "Duck");
        assertFalse(rs.next(), "There should be only one row!");
      }
    }
  }

  @Test
  public void testTwoTablesConcurrently() throws SQLException {
    String tempDir = Utils.getTempDataDirectory().getAbsolutePath();

    String store1 = "store1";
    String store2 = "store2";

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    DuckDBDaVinciRecordTransformer recordTransformerForStore1 = new DuckDBDaVinciRecordTransformer(
        store1,
        1,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        tempDir,
        columnsToProject);
    DuckDBDaVinciRecordTransformer recordTransformerForStore2 = new DuckDBDaVinciRecordTransformer(
        store2,
        1,
        TWO_FIELDS_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        tempDir,
        columnsToProject);

    String duckDBUrl = recordTransformerForStore1.getDuckDBUrl();

    recordTransformerForStore1.onStartVersionIngestion(1, true);

    GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecord.put("key", "key");
    Lazy<GenericRecord> lazyKeyForStore1 = Lazy.of(() -> keyRecord);

    GenericRecord valueRecordForStore1 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecordForStore1.put("firstName", "Duck");
    valueRecordForStore1.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValueForStore1 = Lazy.of(() -> valueRecordForStore1);
    recordTransformerForStore1.processPut(lazyKeyForStore1, lazyValueForStore1, partitionId, null);

    recordTransformerForStore2.onStartVersionIngestion(1, true);

    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      assertDataset1(stmt, store1);
      assertJoin(stmt, store1, store2, false);
    }

    GenericRecord keyRecordForStore2 = new GenericData.Record(TWO_FIELDS_RECORD_SCHEMA);
    keyRecordForStore2.put("id1", 1);
    keyRecordForStore2.put("id2", 2L);
    Lazy<GenericRecord> lazyKeyForStore2 = Lazy.of(() -> keyRecordForStore2);

    GenericRecord valueRecordForStore2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecordForStore2.put("firstName", "Duck");
    valueRecordForStore2.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValueForStore2 = Lazy.of(() -> valueRecordForStore2);
    recordTransformerForStore2.processPut(lazyKeyForStore2, lazyValueForStore2, partitionId, null);

    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      assertDataset1(stmt, store1);

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + store2)) {
        assertTrue(rs.next(), "There should be a first row!");
        assertEquals(rs.getInt("id1"), 1);
        assertEquals(rs.getLong("id2"), 2L);
        assertEquals(rs.getString("firstName"), "Duck");
        assertEquals(rs.getString("lastName"), "Goose");
        assertFalse(rs.next(), "There should be only one row!");
      }

      assertJoin(stmt, store1, store2, true);

      recordTransformerForStore2.processDelete(lazyKeyForStore2, partitionId, null);

      assertJoin(stmt, store1, store2, false);

      recordTransformerForStore1.processDelete(lazyKeyForStore1, partitionId, null);

      try (ResultSet rs = stmt.executeQuery(getJoinQuery(store1, store2))) {
        assertFalse(rs.next());
      }
    }
  }

  private void assertDataset1(Statement statement, String storeName) throws SQLException {
    try (ResultSet rs = statement.executeQuery("SELECT * FROM " + storeName)) {
      assertTrue(rs.next(), "There should be a first row!");
      assertEquals(rs.getString("key"), "key");
      assertEquals(rs.getString("firstName"), "Duck");
      assertEquals(rs.getString("lastName"), "Goose");
      assertFalse(rs.next(), "There should be only one row!");
    }
  }

  private void assertJoin(Statement statement, String store1, String store2, boolean includeStore2)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery(getJoinQuery(store1, store2))) {
      assertTrue(rs.next(), "There should be a first row!");
      assertEquals(rs.getString("s1key"), "key");
      assertEquals(rs.getString("s1FirstName"), "Duck");
      assertEquals(rs.getString("s1LastName"), "Goose");
      assertEquals(rs.getInt("s2id1"), includeStore2 ? 1 : 0);
      assertEquals(rs.getLong("s2id2"), includeStore2 ? 2L : 0);
      assertEquals(rs.getString("s2FirstName"), includeStore2 ? "Duck" : null);
      assertEquals(rs.getString("s2LastName"), includeStore2 ? "Goose" : null);
      assertFalse(rs.next(), "There should be only one row!");
    }
  }

  private String getJoinQuery(String store1, String store2) {
    return "SELECT s1.key AS s1key, s1.firstName AS s1FirstName, s1.lastName AS s1LastName, "
        + "s2.id1 AS s2id1, s2.id2 AS s2id2, s2.firstName AS s2FirstName, s2.lastName AS s2LastName FROM " + store1
        + " s1 LEFT JOIN " + store2 + " s2 ON s1.firstName = s2.firstName AND s1.lastName = s2.lastName";
  }
}
