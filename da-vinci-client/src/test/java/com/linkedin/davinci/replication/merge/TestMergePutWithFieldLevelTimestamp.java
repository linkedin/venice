package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.davinci.replication.merge.TestMergeConflictSchemaConstants.*;
import static com.linkedin.venice.schema.rmd.ReplicationMetadataConstants.*;
import static org.mockito.Mockito.*;


public class TestMergePutWithFieldLevelTimestamp extends TestMergeConflictResolver {

  @Test
  public void testNewPutIgnored() {
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, valueRecordSchemaV1)).when(schemaRepository).getValueSchema(storeName, 1);
    doReturn(new SchemaEntry(2, valueRecordSchemaV2)).when(schemaRepository).getValueSchema(storeName, 2);

    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        schemaRepository,
        new ReplicationMetadataSerDe(schemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );

    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 20L);
    fieldNameToTimestampMap.put("age", 30L);
    GenericRecord rmdRecord = createRmd(rmdSchemaV1, fieldNameToTimestampMap);
    final int oldValueSchemaID = 1;

    MergeConflictResult mergeResult = mergeConflictResolver.put(
        Lazy.of(() -> null),
        Optional.of(new ReplicationMetadataWithValueSchemaId(oldValueSchemaID, RMD_VERSION_ID, rmdRecord)),
        null,
        9L,
        1, // Same as the old value schema ID.
        1L,
        0,
        0
    );
    Assert.assertTrue(mergeResult.isUpdateIgnored());
  }

  @Test
  public void testPutWithFieldLevelTimestamp() {
    final Schema userSchemaV3 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(USER_SCHEMA_STR_V3);
    final Schema userSchemaV4 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(USER_SCHEMA_STR_V4);
    final Schema userSchemaV5 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(USER_SCHEMA_STR_V5);
    // Make sure that schemas used for testing meet the expectation.
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(userSchemaV5, userSchemaV4));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(userSchemaV5, userSchemaV3));
    final Schema rmdSchemaV3 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(userSchemaV3);
    final Schema rmdSchemaV4 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(userSchemaV4);
    final Schema rmdSchemaV5 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(userSchemaV5);
    final ReadOnlySchemaRepository schemaRepository = mockSchemaRepository(
        userSchemaV3, userSchemaV4, userSchemaV5, rmdSchemaV3, rmdSchemaV4, rmdSchemaV5
    );

    // Case 1: Old value and new value have the same schema.
    testOldAndNewValuesHaveSameSchema(userSchemaV3, rmdSchemaV3, schemaRepository);

    // Case 2: Old value and new value have the different schemas and none of them is a superset schema of another.
    testOldAndNewValuesHaveMismatchedSchema(userSchemaV3, rmdSchemaV3, userSchemaV4, userSchemaV5, schemaRepository);

    // Case 3: Old value and new value have the different schemas and new value schema is the superset schema.
    testNewValueSchemaIsSupersetSchema(userSchemaV3, rmdSchemaV3, userSchemaV5, schemaRepository);

    // Case 4: Old value and new value have the different schemas and old value schema is the superset schema.
    testOldValueSchemaIsSupersetSchema(userSchemaV4, userSchemaV5, rmdSchemaV5, schemaRepository);
  }

  private ReadOnlySchemaRepository mockSchemaRepository(
      Schema userSchemaV3,
      Schema userSchemaV4,
      Schema userSchemaV5,
      Schema rmdSchemaV3,
      Schema rmdSchemaV4,
      Schema rmdSchemaV5
  ) {
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(3, userSchemaV3)).when(schemaRepository).getValueSchema(storeName, 3);
    doReturn(new SchemaEntry(4, userSchemaV4)).when(schemaRepository).getValueSchema(storeName, 4);
    doReturn(new SchemaEntry(5, userSchemaV5)).when(schemaRepository).getValueSchema(storeName, 5);
    doReturn(new ReplicationMetadataSchemaEntry(3, RMD_VERSION_ID, rmdSchemaV3))
        .when(schemaRepository)
        .getReplicationMetadataSchema(storeName, 3, RMD_VERSION_ID);

    doReturn(new ReplicationMetadataSchemaEntry(4, RMD_VERSION_ID, rmdSchemaV4))
        .when(schemaRepository)
        .getReplicationMetadataSchema(storeName, 4, RMD_VERSION_ID);

    doReturn(new ReplicationMetadataSchemaEntry(5, RMD_VERSION_ID, rmdSchemaV5))
        .when(schemaRepository)
        .getReplicationMetadataSchema(storeName, 5, RMD_VERSION_ID);

    doReturn(Optional.of(new SchemaEntry(5, userSchemaV5))).when(schemaRepository).getSupersetSchema(storeName);
    return schemaRepository;
  }

  private void testOldAndNewValuesHaveSameSchema(Schema userSchemaV3, Schema rmdSchemaV3, ReadOnlySchemaRepository schemaRepository) {
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 20L);
    GenericRecord oldRmdRecord = createRmd(rmdSchemaV3, fieldNameToTimestampMap); // Existing RMD.
    ReplicationMetadataWithValueSchemaId oldRmdWithValueSchemaID = new ReplicationMetadataWithValueSchemaId(3, RMD_VERSION_ID, oldRmdRecord);
    GenericRecord oldValueRecord = new GenericData.Record(userSchemaV3);
    oldValueRecord.put("id", "123");
    oldValueRecord.put("name", "James");
    final ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV3).serialize(oldValueRecord));

    GenericRecord newValueRecord = new GenericData.Record(userSchemaV3);
    newValueRecord.put("id", "456");
    newValueRecord.put("name", "Lebron");
    ByteBuffer newValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV3).serialize(newValueRecord));

    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        schemaRepository,
        new ReplicationMetadataSerDe(schemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> oldValueBytes),
        Optional.of(oldRmdWithValueSchemaID),
        newValueBytes,
        15L,
        3, // same as the old value schema ID.
        1L,
        0,
        0
    );

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedRmd = result.getReplicationMetadataRecord();
    GenericRecord updatedPerFieldTimestampRecord = (GenericRecord) updatedRmd.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("id"), 15L);   // Updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("name"), 20L); // Not updated

    Optional<ByteBuffer> newValueOptional = result.getNewValue();
    Assert.assertTrue(newValueOptional.isPresent());
    newValueRecord = getDeserializer(userSchemaV3, userSchemaV3).deserialize(newValueOptional.get());
    Assert.assertEquals(newValueRecord.get("id").toString(), "456");     // Updated
    Assert.assertEquals(newValueRecord.get("name").toString(), "James"); // Not updated
  }

  private void testOldAndNewValuesHaveMismatchedSchema(
      Schema userSchemaV3,
      Schema rmdSchemaV3,
      Schema userSchemaV4,
      Schema userSchemaV5,
      ReadOnlySchemaRepository schemaRepository
  ) {
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 20L);
    GenericRecord oldRmdRecord = createRmd(rmdSchemaV3, fieldNameToTimestampMap); // Existing RMD.
    ReplicationMetadataWithValueSchemaId oldRmdWithValueSchemaID = new ReplicationMetadataWithValueSchemaId(3, RMD_VERSION_ID, oldRmdRecord);
    GenericRecord oldValueRecord = new GenericData.Record(userSchemaV3);
    oldValueRecord.put("id", "123");
    oldValueRecord.put("name", "James");
    final ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV3).serialize(oldValueRecord));

    GenericRecord newValueRecord = new GenericData.Record(userSchemaV4);
    newValueRecord.put("name", "Lebron");
    newValueRecord.put("weight", 250.0f);
    ByteBuffer newValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV4).serialize(newValueRecord));

    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        schemaRepository,
        new ReplicationMetadataSerDe(schemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> oldValueBytes),
        Optional.of(oldRmdWithValueSchemaID),
        newValueBytes,
        15L,
        4, // new value schema ID is different from the old value schema ID.
        1L,
        0,
        0
    );

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedRmd = result.getReplicationMetadataRecord();
    GenericRecord updatedPerFieldTimestampRecord = (GenericRecord) updatedRmd.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("id"), 10L);   // Not updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("name"), 20L); // Not updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("weight"), 15L); // Not updated and it is a new field.

    Optional<ByteBuffer> newValueOptional = result.getNewValue();
    Assert.assertTrue(newValueOptional.isPresent());
    newValueRecord = getDeserializer(userSchemaV5, userSchemaV5).deserialize(newValueOptional.get());
    Assert.assertEquals(newValueRecord.get("id").toString(), "123");     // Not updated
    Assert.assertEquals(newValueRecord.get("name").toString(), "James"); // Not updated
    Assert.assertEquals(newValueRecord.get("weight").toString(), "250.0"); // Updated and it is a new field.
  }

  private void testNewValueSchemaIsSupersetSchema(
      Schema userSchemaV3,
      Schema rmdSchemaV3,
      Schema userSchemaV5,
      ReadOnlySchemaRepository schemaRepository
  ) {
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 20L);
    GenericRecord oldRmdRecord = createRmd(rmdSchemaV3, fieldNameToTimestampMap); // Existing RMD
    ReplicationMetadataWithValueSchemaId oldRmdWithValueSchemaID = new ReplicationMetadataWithValueSchemaId(3, RMD_VERSION_ID, oldRmdRecord);
    GenericRecord oldValueRecord = new GenericData.Record(userSchemaV3);
    oldValueRecord.put("id", "123");
    oldValueRecord.put("name", "James");
    final ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV3).serialize(oldValueRecord));

    GenericRecord newValueRecord = new GenericData.Record(userSchemaV5);
    newValueRecord.put("id", "456");
    newValueRecord.put("name", "Lebron");
    newValueRecord.put("weight", 250.0f); // New field
    ByteBuffer newValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV5).serialize(newValueRecord));

    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        schemaRepository,
        new ReplicationMetadataSerDe(schemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> oldValueBytes),
        Optional.of(oldRmdWithValueSchemaID),
        newValueBytes,
        15L,
        5, // new value schema ID is different from the old value schema ID.
        1L,
        0,
        0
    );

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedRmd = result.getReplicationMetadataRecord();
    GenericRecord updatedPerFieldTimestampRecord = (GenericRecord) updatedRmd.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("id"), 15L);   // Updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("name"), 20L); // Not updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("weight"), 15L); // Not updated and it is a new field.

    Optional<ByteBuffer> newValueOptional = result.getNewValue();
    Assert.assertTrue(newValueOptional.isPresent());
    newValueRecord = getDeserializer(userSchemaV5, userSchemaV5).deserialize(newValueOptional.get());
    Assert.assertEquals(newValueRecord.get("id").toString(), "456");     // Not updated
    Assert.assertEquals(newValueRecord.get("name").toString(), "James"); // Not updated
    Assert.assertEquals(newValueRecord.get("weight").toString(), "250.0"); // Updated and it is a new field.
  }

  private void testOldValueSchemaIsSupersetSchema(
      Schema userSchemaV4,
      Schema userSchemaV5,
      Schema rmdSchemaV5,
      ReadOnlySchemaRepository schemaRepository
  ) {
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 20L);
    fieldNameToTimestampMap.put("weight", 30L);
    GenericRecord oldRmdRecord = createRmd(rmdSchemaV5, fieldNameToTimestampMap); // Existing RMD
    ReplicationMetadataWithValueSchemaId oldRmdWithValueSchemaID = new ReplicationMetadataWithValueSchemaId(5, RMD_VERSION_ID, oldRmdRecord);
    GenericRecord oldValueRecord = new GenericData.Record(userSchemaV5);
    oldValueRecord.put("id", "123");
    oldValueRecord.put("name", "James");
    oldValueRecord.put("weight", 250.1f);
    final ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV5).serialize(oldValueRecord));

    GenericRecord newValueRecord = new GenericData.Record(userSchemaV4);
    newValueRecord.put("name", "Lebron");
    newValueRecord.put("weight", 230.0f); // Different field value
    ByteBuffer newValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV4).serialize(newValueRecord));

    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        schemaRepository,
        new ReplicationMetadataSerDe(schemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> oldValueBytes),
        Optional.of(oldRmdWithValueSchemaID),
        newValueBytes,
        25L,
        4, // new value schema ID is different from the old value schema ID.
        1L,
        0,
        0
    );

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedRmd = result.getReplicationMetadataRecord();
    GenericRecord updatedPerFieldTimestampRecord = (GenericRecord) updatedRmd.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("id"), 10L);   // Not updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("name"), 25L); // Updated
    Assert.assertEquals(updatedPerFieldTimestampRecord.get("weight"), 30L); // Not updated

    Optional<ByteBuffer> newValueOptional = result.getNewValue();
    Assert.assertTrue(newValueOptional.isPresent());
    newValueRecord = getDeserializer(userSchemaV5, userSchemaV5).deserialize(newValueOptional.get());
    Assert.assertEquals(newValueRecord.get("id").toString(), "123");     // Not updated
    Assert.assertEquals(newValueRecord.get("name").toString(), "Lebron"); // Updated
    Assert.assertEquals(newValueRecord.get("weight").toString(), "250.1"); // Updated and it is a new field.
  }
}
