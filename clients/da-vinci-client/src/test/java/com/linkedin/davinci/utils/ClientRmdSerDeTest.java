package com.linkedin.davinci.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ClientRmdSerDeTest {
  private static final int VALUE_SCHEMA_V1_ID = 1;
  private static final int VALUE_SCHEMA_V2_ID = 2;

  // Value schema v1 - basic user record
  private static final String VALUE_SCHEMA_V1_STR = "{" + "\"type\": \"record\"," + "\"name\": \"User\","
      + "\"namespace\": \"com.linkedin.test\"," + "\"fields\": [" + "  {\"name\": \"id\", \"type\": \"string\"},"
      + "  {\"name\": \"name\", \"type\": \"string\"}," + "  {\"name\": \"age\", \"type\": \"int\"}" + "]}";

  // Value schema v2 - adds email field with default
  private static final String VALUE_SCHEMA_V2_STR = "{" + "\"type\": \"record\"," + "\"name\": \"User\","
      + "\"namespace\": \"com.linkedin.test\"," + "\"fields\": [" + "  {\"name\": \"id\", \"type\": \"string\"},"
      + "  {\"name\": \"name\", \"type\": \"string\"}," + "  {\"name\": \"age\", \"type\": \"int\"},"
      + "  {\"name\": \"email\", \"type\": \"string\", \"default\": \"\"}" + "]}";

  private Schema valueSchemaV1;
  private Schema valueSchemaV2;
  private Schema rmdSchemaV1;
  private Schema rmdSchemaV2;
  private StoreSchemaFetcher mockSchemaFetcher;
  private ClientRmdSerDe clientRmdSerDe;
  private ClientRmdSerDe clientRmdSerDeFastAvroDisabled;
  private RmdSchemaGeneratorV1 rmdGenerator;

  @BeforeMethod
  public void setUp() {
    // Parse value schemas
    valueSchemaV1 = AvroCompatibilityHelper.parse(VALUE_SCHEMA_V1_STR);
    valueSchemaV2 = AvroCompatibilityHelper.parse(VALUE_SCHEMA_V2_STR);

    // Generate RMD schemas
    rmdGenerator = new RmdSchemaGeneratorV1();
    rmdSchemaV1 = rmdGenerator.generateMetadataSchema(valueSchemaV1);
    rmdSchemaV2 = rmdGenerator.generateMetadataSchema(valueSchemaV2);

    // Mock StoreSchemaFetcher
    mockSchemaFetcher = mock(StoreSchemaFetcher.class);
    when(mockSchemaFetcher.getValueSchema(VALUE_SCHEMA_V1_ID)).thenReturn(valueSchemaV1);
    when(mockSchemaFetcher.getValueSchema(VALUE_SCHEMA_V2_ID)).thenReturn(valueSchemaV2);

    // Create ClientRmdSerDe instances
    clientRmdSerDe = new ClientRmdSerDe(mockSchemaFetcher, true); // fast avro enabled
    clientRmdSerDeFastAvroDisabled = new ClientRmdSerDe(mockSchemaFetcher, false); // fast avro disabled
  }

  @Test
  public void testGetRmdSchemaForDifferentValueSchemaVersions() {
    // Test RMD schema generation for v1
    Schema actualRmdSchemaV1 = clientRmdSerDe.getRmdSchema(VALUE_SCHEMA_V1_ID);
    Assert.assertNotNull(actualRmdSchemaV1);
    Assert.assertEquals(actualRmdSchemaV1, rmdSchemaV1);

    // Test RMD schema generation for v2
    Schema actualRmdSchemaV2 = clientRmdSerDe.getRmdSchema(VALUE_SCHEMA_V2_ID);
    Assert.assertNotNull(actualRmdSchemaV2);
    Assert.assertEquals(actualRmdSchemaV2, rmdSchemaV2);

    // Verify schemas are different (v2 should have additional field metadata)
    Assert.assertNotEquals(actualRmdSchemaV1, actualRmdSchemaV2);
  }

  @Test
  public void testSerializeAndDeserializeRmdRecordV1() {
    // Create RMD record for v1 schema
    GenericRecord rmdRecordV1 = createRmdRecord(rmdSchemaV1, 1000L);

    // Serialize RMD record
    ByteBuffer serializedRmd = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecordV1);
    Assert.assertNotNull(serializedRmd);
    Assert.assertTrue(serializedRmd.remaining() > 0);

    // Deserialize RMD record (same schema version)
    GenericRecord deserializedRmd =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V1_ID, serializedRmd);
    Assert.assertNotNull(deserializedRmd);

    // Verify deserialized record matches original
    Assert.assertEquals(
        deserializedRmd.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        rmdRecordV1.get(RmdConstants.TIMESTAMP_FIELD_NAME));
    Assert.assertEquals(
        deserializedRmd.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME),
        rmdRecordV1.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME));

    // Verify checkpoint vector has correct size and values for V1 (3 fields)
    List<Long> checkpointVector =
        (List<Long>) deserializedRmd.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(checkpointVector.size(), 3, "V1 checkpoint vector should have 3 elements");
    Assert.assertTrue(
        checkpointVector.stream().allMatch(val -> val != null && val >= 1000L),
        "All checkpoint values should be non-null and >= base timestamp");
  }

  @Test
  public void testSerializeAndDeserializeRmdRecordV2() {
    // Create RMD record for v2 schema
    GenericRecord rmdRecordV2 = createRmdRecord(rmdSchemaV2, 2000L);

    // Serialize RMD record
    ByteBuffer serializedRmd = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V2_ID, rmdRecordV2);
    Assert.assertNotNull(serializedRmd);
    Assert.assertTrue(serializedRmd.remaining() > 0);

    // Deserialize RMD record (same schema version)
    GenericRecord deserializedRmd =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V2_ID, VALUE_SCHEMA_V2_ID, serializedRmd);
    Assert.assertNotNull(deserializedRmd);

    // Verify deserialized record matches original
    Assert.assertEquals(
        deserializedRmd.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        rmdRecordV2.get(RmdConstants.TIMESTAMP_FIELD_NAME));
    Assert.assertEquals(
        deserializedRmd.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME),
        rmdRecordV2.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME));

    // Verify checkpoint vector has correct size and values for V2 (4 fields)
    List<Long> checkpointVector =
        (List<Long>) deserializedRmd.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(checkpointVector.size(), 4, "V2 checkpoint vector should have 4 elements");
    Assert.assertTrue(
        checkpointVector.stream().allMatch(val -> val != null && val >= 2000L),
        "All checkpoint values should be non-null and >= base timestamp");
  }

  @Test
  public void testCrossVersionDeserialization() {
    // Create RMD record for v1 schema
    GenericRecord rmdRecordV1 = createRmdRecord(rmdSchemaV1, 1500L);
    ByteBuffer serializedRmdV1 = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecordV1);

    // Create RMD record for v2 schema
    GenericRecord rmdRecordV2 = createRmdRecord(rmdSchemaV2, 2500L);
    ByteBuffer serializedRmdV2 = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V2_ID, rmdRecordV2);

    // Test deserializing v1 RMD with v2 reader schema (forward compatibility)
    GenericRecord deserializedV1WithV2Reader =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V2_ID, serializedRmdV1);
    Assert.assertNotNull(deserializedV1WithV2Reader);
    Assert.assertEquals(
        deserializedV1WithV2Reader.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        rmdRecordV1.get(RmdConstants.TIMESTAMP_FIELD_NAME));

    // Verify checkpoint vector is preserved in forward compatibility
    List<Long> forwardCheckpointVector =
        (List<Long>) deserializedV1WithV2Reader.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertNotNull(forwardCheckpointVector);
    Assert
        .assertEquals(forwardCheckpointVector, rmdRecordV1.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME));

    // Test deserializing v2 RMD with v1 reader schema (backward compatibility)
    GenericRecord deserializedV2WithV1Reader =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V2_ID, VALUE_SCHEMA_V1_ID, serializedRmdV2);
    Assert.assertNotNull(deserializedV2WithV1Reader);
    Assert.assertEquals(
        deserializedV2WithV1Reader.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        rmdRecordV2.get(RmdConstants.TIMESTAMP_FIELD_NAME));

    // Verify checkpoint vector is preserved in backward compatibility
    List<Long> backwardCheckpointVector =
        (List<Long>) deserializedV2WithV1Reader.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertNotNull(backwardCheckpointVector);
    Assert
        .assertEquals(backwardCheckpointVector, rmdRecordV2.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME));
  }

  @Test
  public void testFastAvroEnabledVsDisabled() {
    // Create RMD record
    GenericRecord rmdRecord = createRmdRecord(rmdSchemaV1, 3000L);

    // Serialize with fast avro enabled
    ByteBuffer serializedWithFastAvro = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecord);

    // Serialize with fast avro disabled
    ByteBuffer serializedWithoutFastAvro =
        clientRmdSerDeFastAvroDisabled.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecord);

    // Both should produce valid serialized data
    Assert.assertNotNull(serializedWithFastAvro);
    Assert.assertNotNull(serializedWithoutFastAvro);
    Assert.assertTrue(serializedWithFastAvro.remaining() > 0);
    Assert.assertTrue(serializedWithoutFastAvro.remaining() > 0);

    // Deserialize both with fast avro enabled client
    GenericRecord deserializedFromFastAvro =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V1_ID, serializedWithFastAvro);
    GenericRecord deserializedFromRegular =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V1_ID, serializedWithoutFastAvro);

    // Both should deserialize correctly
    Assert.assertNotNull(deserializedFromFastAvro);
    Assert.assertNotNull(deserializedFromRegular);
    Assert.assertEquals(
        deserializedFromFastAvro.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        rmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME));
    Assert.assertEquals(
        deserializedFromRegular.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        rmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME));
  }

  @Test
  public void testSchemaEvolutionWithNewField() {
    // Create RMD records for both schema versions
    GenericRecord rmdRecordV1 = createRmdRecord(rmdSchemaV1, 4000L);
    GenericRecord rmdRecordV2 = createRmdRecord(rmdSchemaV2, 5000L);

    // Verify that v2 RMD schema has more fields than v1 (due to new email field in value schema)
    Assert.assertTrue(rmdSchemaV2.getFields().size() >= rmdSchemaV1.getFields().size());

    // Serialize both
    ByteBuffer serializedV1 = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecordV1);
    ByteBuffer serializedV2 = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V2_ID, rmdRecordV2);

    // Cross-version deserialization should work
    GenericRecord v1DeserializedAsV2 =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V2_ID, serializedV1);
    GenericRecord v2DeserializedAsV1 =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V2_ID, VALUE_SCHEMA_V1_ID, serializedV2);

    Assert.assertNotNull(v1DeserializedAsV2);
    Assert.assertNotNull(v2DeserializedAsV1);

    // Verify timestamps are preserved across schema evolution
    Assert.assertEquals(v1DeserializedAsV2.get(RmdConstants.TIMESTAMP_FIELD_NAME), 4000L);
    Assert.assertEquals(v2DeserializedAsV1.get(RmdConstants.TIMESTAMP_FIELD_NAME), 5000L);
  }

  @Test
  public void testReplicationCheckpointVectorVerification() {
    // Test V1 schema (3 fields: id, name, age)
    GenericRecord rmdRecordV1 = createRmdRecord(rmdSchemaV1, 1000L);
    List<Long> checkpointVectorV1 = (List<Long>) rmdRecordV1.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);

    // V1 schema has 3 fields, so checkpoint vector should have 3 elements
    Assert.assertNotNull(checkpointVectorV1);
    Assert.assertEquals(checkpointVectorV1.size(), 3, "V1 checkpoint vector should have 3 elements for 3 fields");
    Assert.assertEquals(checkpointVectorV1.get(0), Long.valueOf(1000L));
    Assert.assertEquals(checkpointVectorV1.get(1), Long.valueOf(1100L));
    Assert.assertEquals(checkpointVectorV1.get(2), Long.valueOf(1200L));

    // Test V2 schema (4 fields: id, name, age, email)
    GenericRecord rmdRecordV2 = createRmdRecord(rmdSchemaV2, 2000L);
    List<Long> checkpointVectorV2 = (List<Long>) rmdRecordV2.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);

    // V2 schema has 4 fields, so checkpoint vector should have 4 elements
    Assert.assertNotNull(checkpointVectorV2);
    Assert.assertEquals(checkpointVectorV2.size(), 4, "V2 checkpoint vector should have 4 elements for 4 fields");
    Assert.assertEquals(checkpointVectorV2.get(0), Long.valueOf(2000L));
    Assert.assertEquals(checkpointVectorV2.get(1), Long.valueOf(2100L));
    Assert.assertEquals(checkpointVectorV2.get(2), Long.valueOf(2200L));
    Assert.assertEquals(checkpointVectorV2.get(3), Long.valueOf(2300L));

    // Test serialization/deserialization preserves checkpoint vector
    ByteBuffer serializedV1 = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecordV1);
    GenericRecord deserializedV1 =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V1_ID, serializedV1);

    List<Long> deserializedCheckpointVectorV1 =
        (List<Long>) deserializedV1.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(
        deserializedCheckpointVectorV1,
        checkpointVectorV1,
        "Deserialized checkpoint vector should match original");

    // Test cross-version deserialization with checkpoint vector
    ByteBuffer serializedV2 = clientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V2_ID, rmdRecordV2);
    GenericRecord deserializedV2AsV1 =
        clientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V2_ID, VALUE_SCHEMA_V1_ID, serializedV2);

    List<Long> crossVersionCheckpointVector =
        (List<Long>) deserializedV2AsV1.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert
        .assertNotNull(crossVersionCheckpointVector, "Cross-version deserialization should preserve checkpoint vector");
    // The checkpoint vector should be preserved even in cross-version scenarios
    Assert.assertEquals(
        crossVersionCheckpointVector,
        checkpointVectorV2,
        "Cross-version checkpoint vector should be preserved");
  }

  @Test
  public void testDefaultConstructor() {
    // Test default constructor (should enable fast avro by default)
    ClientRmdSerDe defaultClientRmdSerDe = new ClientRmdSerDe(mockSchemaFetcher);

    // Should be able to get RMD schema
    Schema rmdSchema = defaultClientRmdSerDe.getRmdSchema(VALUE_SCHEMA_V1_ID);
    Assert.assertNotNull(rmdSchema);
    Assert.assertEquals(rmdSchema, rmdSchemaV1);

    // Should be able to serialize/deserialize
    GenericRecord rmdRecord = createRmdRecord(rmdSchemaV1, 6000L);
    ByteBuffer serialized = defaultClientRmdSerDe.serializeRmdRecord(VALUE_SCHEMA_V1_ID, rmdRecord);
    GenericRecord deserialized =
        defaultClientRmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_V1_ID, VALUE_SCHEMA_V1_ID, serialized);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(deserialized.get(RmdConstants.TIMESTAMP_FIELD_NAME), 6000L);
  }

  /**
   * Helper method to create a basic RMD record with the given schema and timestamp.
   * Creates a replication checkpoint vector with size equal to the number of fields in the corresponding value schema.
   */
  private GenericRecord createRmdRecord(Schema rmdSchema, long timestamp) {
    GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, timestamp);

    // Create replication checkpoint vector based on the schema
    // For testing purposes, we'll determine the number of fields from the RMD schema structure
    List<Long> checkpointVector = createCheckpointVector(rmdSchema, timestamp);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, checkpointVector);

    return rmdRecord;
  }

  /**
   * Helper method to create a replication checkpoint vector.
   * The size should match the number of fields in the corresponding value schema.
   */
  private List<Long> createCheckpointVector(Schema rmdSchema, long baseTimestamp) {
    // Determine the number of fields by examining the timestamp field structure
    Schema timestampField = rmdSchema.getField(RmdConstants.TIMESTAMP_FIELD_NAME).schema();

    int numFields;
    if (timestampField.getType() == Schema.Type.UNION) {
      // If timestamp is a union, check if it contains a record (field-level timestamps)
      Schema recordSchema = null;
      for (Schema unionType: timestampField.getTypes()) {
        if (unionType.getType() == Schema.Type.RECORD) {
          recordSchema = unionType;
          break;
        }
      }
      numFields = recordSchema != null ? recordSchema.getFields().size() : 1;
    } else {
      // Simple timestamp, assume single field
      numFields = 1;
    }

    // Create checkpoint vector with meaningful values
    List<Long> checkpointVector = new ArrayList<>();
    for (int i = 0; i < numFields; i++) {
      checkpointVector.add(baseTimestamp + i * 100L); // Stagger the checkpoint values
    }

    return checkpointVector;
  }
}
