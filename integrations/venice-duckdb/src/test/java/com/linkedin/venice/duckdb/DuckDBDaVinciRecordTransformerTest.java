package com.linkedin.venice.duckdb;

import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.SINGLE_FIELD_RECORD_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.DaVinciRecordTransformerUtility;
import com.linkedin.venice.sql.DuckDBDaVinciRecordTransformer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DuckDBDaVinciRecordTransformerTest {
  static final int storeVersion = 1;

  @BeforeMethod
  @AfterClass
  public void deleteClassHash() {
    File file = new File(String.format("./classHash-%d.txt", storeVersion));
    if (file.exists()) {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testRecordTransformer() {
    String tempDir = Utils.getTempDataDirectory().getAbsolutePath();
    DuckDBDaVinciRecordTransformer recordTransformer = new DuckDBDaVinciRecordTransformer(storeVersion, false, tempDir);

    Schema keySchema = recordTransformer.getKeySchema();
    assertEquals(keySchema.getType(), Schema.Type.RECORD);

    Schema outputValueSchema = recordTransformer.getOutputValueSchema();
    assertEquals(outputValueSchema.getType(), Schema.Type.RECORD);

    recordTransformer.onStartVersionIngestion(true);

    GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecord.put("key", "key");
    Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

    GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord.put("firstName", "Duck");
    valueRecord.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord);

    DaVinciRecordTransformerResult<GenericRecord> transformerResult = recordTransformer.transform(lazyKey, lazyValue);
    recordTransformer.processPut(lazyKey, lazyValue);
    assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.UNCHANGED);
    // Result will be empty when it's UNCHANGED
    assertNull(transformerResult.getValue());
    assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue));

    recordTransformer.processDelete(lazyKey);

    assertFalse(recordTransformer.getStoreRecordsInDaVinci());

    int classHash = recordTransformer.getClassHash();

    DaVinciRecordTransformerUtility<GenericRecord, GenericRecord> recordTransformerUtility =
        recordTransformer.getRecordTransformerUtility();
    assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash));
    assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash));
  }

  @Test
  public void testVersionSwap() throws SQLException {
    String tempDir = Utils.getTempDataDirectory().getAbsolutePath();
    DuckDBDaVinciRecordTransformer recordTransformer_v1 = new DuckDBDaVinciRecordTransformer(1, false, tempDir);
    DuckDBDaVinciRecordTransformer recordTransformer_v2 = new DuckDBDaVinciRecordTransformer(2, false, tempDir);

    String duckDBUrl = recordTransformer_v1.getDuckDBUrl();

    recordTransformer_v1.onStartVersionIngestion(true);
    recordTransformer_v2.onStartVersionIngestion(false);

    GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    keyRecord.put("key", "key");
    Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

    GenericRecord valueRecord_v1 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord_v1.put("firstName", "Duck");
    valueRecord_v1.put("lastName", "Goose");
    Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord_v1);
    recordTransformer_v1.processPut(lazyKey, lazyValue);

    GenericRecord valueRecord_v2 = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord_v2.put("firstName", "Goose");
    valueRecord_v2.put("lastName", "Duck");
    lazyValue = Lazy.of(() -> valueRecord_v2);
    recordTransformer_v2.processPut(lazyKey, lazyValue);

    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM current_version")) {
        assertTrue(rs.next(), "There should be a first row!");
        assertEquals(rs.getString("firstName"), "Duck");
        assertEquals(rs.getString("lastName"), "Goose");
        assertFalse(rs.next(), "There should be only one row!");
      }

      // Swap here
      recordTransformer_v1.onEndVersionIngestion(2);

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM current_version")) {
        assertTrue(rs.next(), "There should be a first row!");
        assertEquals(rs.getString("firstName"), "Goose");
        assertEquals(rs.getString("lastName"), "Duck");
        assertFalse(rs.next(), "There should be only one row!");
      }
    }
  }
}
