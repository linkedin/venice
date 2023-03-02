package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveLongList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestMergeWithValueLevelTimestamp extends TestMergeConflictResolver {
  @Test
  public void testPut() {
    GenericRecord oldValueRecord = new GenericData.Record(userSchemaV1);
    oldValueRecord.put("id", "id1");
    oldValueRecord.put("name", "name1");
    oldValueRecord.put("age", 10);

    GenericRecord rmdRecord = new GenericData.Record(userRmdSchemaV1);
    rmdRecord.put(TIMESTAMP_FIELD_NAME, 20L);
    rmdRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, Collections.emptyList());

    GenericRecord newValueRecord = new GenericData.Record(userSchemaV1);
    newValueRecord.put("id", "id10");
    newValueRecord.put("name", "name10");
    newValueRecord.put("age", 20);

    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);
    ByteBuffer oldBB = serialize(oldValueRecord);
    ByteBuffer newBB = serialize(newValueRecord);
    MergeConflictResult mergeConflictResult = mergeConflictResolver.put(
        Lazy.of(() -> oldBB),
        new RmdWithValueSchemaId(1, RMD_VERSION_ID, GenericData.get().deepCopy(userRmdSchemaV1, rmdRecord)),
        newBB,
        30L,
        1,
        1L,
        0,
        0);

    // verify id and name fields are from new record
    GenericRecord result = deserializer.deserialize(mergeConflictResult.getNewValue());
    Assert.assertEquals(GenericData.get().compare(result, newValueRecord, userSchemaV1), 0);

    // verify update ignored.
    mergeConflictResult = mergeConflictResolver.put(
        Lazy.of(() -> oldBB),
        new RmdWithValueSchemaId(1, RMD_VERSION_ID, GenericData.get().deepCopy(userRmdSchemaV1, rmdRecord)),
        newBB,
        10L,
        1,
        1L,
        0,
        0);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // verify same timestamp case
    mergeConflictResult = mergeConflictResolver.put(
        Lazy.of(() -> oldBB),
        new RmdWithValueSchemaId(1, RMD_VERSION_ID, GenericData.get().deepCopy(userRmdSchemaV1, rmdRecord)),
        newBB,
        20L,
        1,
        1L,
        0,
        0);

    if (MergeUtils.compareAndReturn(oldBB, newBB) == oldBB) {
      Assert.assertNull(mergeConflictResult.getNewValue());
    } else {
      Assert.assertEquals(mergeConflictResult.getNewValue(), newBB);
    }

    // verify overwrite with new value case
    mergeConflictResult = mergeConflictResolver.put(
        Lazy.of(() -> oldBB),
        new RmdWithValueSchemaId(1, RMD_VERSION_ID, GenericData.get().deepCopy(userRmdSchemaV1, rmdRecord)),
        newBB,
        30L,
        1,
        1L,
        0,
        0);
    Assert.assertEquals(mergeConflictResult.getNewValue(), newBB);

    // verify put with invalid schema id
    Assert.assertThrows(
        VeniceException.class,
        () -> mergeConflictResolver.put(
            Lazy.of(() -> oldBB),
            new RmdWithValueSchemaId(-1, RMD_VERSION_ID, rmdRecord),
            newBB,
            30L,
            1,
            1L,
            0,
            0));

    // validate null old value
    mergeConflictResult = mergeConflictResolver.put(
        Lazy.of(() -> null),
        new RmdWithValueSchemaId(1, RMD_VERSION_ID, GenericData.get().deepCopy(userRmdSchemaV1, rmdRecord)),
        newBB,
        30L,
        1,
        1L,
        0,
        0);
    result = deserializer.deserialize(mergeConflictResult.getNewValue());
    Assert.assertEquals(GenericData.get().compare(result, newValueRecord, userSchemaV1), 0);

    // validate null old value BUT with an existing timestamp telling us that this was a deleted record, deletes should
    // win on a tie, meaning
    // this should get an ignore result
    mergeConflictResult = mergeConflictResolver
        .put(Lazy.of(() -> null), new RmdWithValueSchemaId(1, RMD_VERSION_ID, rmdRecord), newBB, 20L, 1, 1L, 0, 0);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // Validate null RMD for existing old value
    mergeConflictResult = mergeConflictResolver.put(Lazy.of(() -> oldBB), null, newBB, 30L, 1, 1L, 0, 0);
    Assert.assertEquals(mergeConflictResult.getNewValue(), newBB);
  }

  @Test
  public void testDelete() {
    GenericRecord valueRecord = new GenericData.Record(userSchemaV1);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timestampRecord = new GenericData.Record(userRmdSchemaV1);
    GenericRecord ts = new GenericData.Record(userRmdSchemaV1.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);

    timestampRecord.put(0, 20L);
    timestampRecord.put(1, new ArrayList<Long>());

    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);
    // new MergeConflictResolver(schemaRepository, storeName, valueSchemaID -> new GenericData.Record(rmdSchemaV1));

    MergeConflictResult mergeConflictResult = mergeConflictResolver
        .delete(Lazy.of(() -> null), new RmdWithValueSchemaId(1, RMD_VERSION_ID, timestampRecord), 30L, 1L, 0, 0);

    // verify delete null value
    Assert.assertNull(mergeConflictResult.getNewValue());

    // verify update ignored.
    mergeConflictResult = mergeConflictResolver
        .delete(Lazy.of(() -> null), new RmdWithValueSchemaId(1, RMD_VERSION_ID, timestampRecord), 10L, 1L, 0, 0);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // verify same timestamp case
    mergeConflictResult = mergeConflictResolver
        .delete(Lazy.of(() -> null), new RmdWithValueSchemaId(1, RMD_VERSION_ID, timestampRecord), 30L, 1L, 0, 0);
    Assert.assertNull(mergeConflictResult.getNewValue());

    // Validate null RMD for existing old value
    mergeConflictResult = mergeConflictResolver.delete(Lazy.of(() -> null), null, 30, 1, 0, 0);
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    Assert.assertNull(mergeConflictResult.getNewValue());

    // Validate null RMD for invalid schema id
    mergeConflictResult = mergeConflictResolver.delete(Lazy.of(() -> null), null, 30, 1, 0, 0);
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    Assert.assertEquals(mergeConflictResult.getValueSchemaId(), 1);

    // Validate delete wins on same timestamp
    mergeConflictResult = mergeConflictResolver
        .delete(Lazy.of(() -> null), new RmdWithValueSchemaId(1, RMD_VERSION_ID, timestampRecord), 30L, 1L, 0, 0);
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    Assert.assertEquals(mergeConflictResult.getValueSchemaId(), 1);

    // verify invalid schema id for existing old value
    Assert.assertThrows(
        VeniceException.class,
        () -> mergeConflictResolver
            .delete(Lazy.of(() -> null), new RmdWithValueSchemaId(-1, RMD_VERSION_ID, timestampRecord), 30L, 1L, 0, 0));
  }

  @Test
  public void testPermutation() {
    List<Long> writeTs = new ArrayList<>();
    List<GenericRecord> payload = new ArrayList<>();
    List<GenericRecord> tsRecord = new ArrayList<>();

    GenericRecord origRecord = new GenericData.Record(userSchemaV1);
    origRecord.put("id", "id0");
    origRecord.put("name", "name0");
    origRecord.put("age", 10);

    for (int i = 1; i <= 100; i++) {
      GenericRecord record = new GenericData.Record(userSchemaV1);
      record.put("id", "id" + i);
      record.put("name", "name" + i);
      record.put("age", 10 + i);
      payload.add(record);
      GenericRecord timeStampRecord = new GenericData.Record(userRmdSchemaV1);
      timeStampRecord.put(0, (long) (i + 10));
      timeStampRecord.put(1, new ArrayList<Long>());
      tsRecord.add(timeStampRecord);
      writeTs.add((long) (i + 15));
    }

    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);
    // new MergeConflictResolver(schemaRepository, storeName, null);
    MergeConflictResult mergeConflictResult = null;
    ByteBuffer oldBB = serialize(origRecord);

    for (int i = 0; i < 100; i++) {
      ByteBuffer newBB = serialize(payload.get(i));
      for (int j = 0; j < 100; j++) {
        GenericRecord rmd = GenericData.get().deepCopy(userRmdSchemaV1, tsRecord.get(j));
        mergeConflictResult = mergeConflictResolver.put(
            Lazy.of(() -> oldBB),
            new RmdWithValueSchemaId(1, RMD_VERSION_ID, rmd),
            newBB,
            writeTs.get(j),
            1,
            1,
            0,
            0);
      }
    }

    GenericRecord result1 = deserializer.deserialize(mergeConflictResult.getNewValue());

    Assert.assertEquals((long) (mergeConflictResult.getRmdRecord()).get(0), 115L);

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        ByteBuffer newBB = serialize(payload.get(j));
        GenericRecord rmd = GenericData.get().deepCopy(userRmdSchemaV1, tsRecord.get(j));
        mergeConflictResult = mergeConflictResolver.put(
            Lazy.of(() -> oldBB),
            new RmdWithValueSchemaId(1, RMD_VERSION_ID, rmd),
            newBB,
            writeTs.get(i),
            1,
            1,
            0,
            0);
      }
    }
    GenericRecord result2 = deserializer.deserialize(mergeConflictResult.getNewValue());

    // validate order of operation change results in a same object
    Assert.assertEquals((long) (mergeConflictResult.getRmdRecord()).get(0), 115L);
    Assert.assertEquals(GenericData.get().compare(result1, result2, userSchemaV1), 0);
  }

  /**
   * Data provider which provides for many list implementations because we've been hurt before :'(
   */
  @DataProvider(name = "Long-Lists-and-null")
  public static Object[][] listImplementationsProvider() {
    return new Object[][] { { new ArrayList<Long>() }, { new PrimitiveLongArrayList(0) },
        { new ColdPrimitiveLongList(0) }, { null } };
  }

  @Test(dataProvider = "Long-Lists-and-null")
  public void testOffsetVectorMergeAndSum(List<Long> newVector) {
    newVector = MergeUtils.mergeOffsetVectors(newVector, 1L, 0);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 2L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 3L, 4);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 7L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 8L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 9L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 3L, 5);
    List<Long> expectedVector = Arrays.asList(1L, 9L, 0L, 0L, 3L, 3L);
    Assert.assertEquals(newVector, expectedVector);
    Assert.assertEquals(RmdUtils.sumOffsetVector(newVector), 16L);

    newVector.clear();
    newVector = MergeUtils.mergeOffsetVectors(newVector, 3L, 5);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 9L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 1L, 0);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 2L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 3L, 4);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 7L, 1);
    newVector = MergeUtils.mergeOffsetVectors(newVector, 8L, 1);
    expectedVector = Arrays.asList(1L, 8L, 0L, 0L, 3L, 3L);
    Assert.assertEquals(newVector, expectedVector);
    Assert.assertEquals(RmdUtils.sumOffsetVector(newVector), 15L);
  }

  private ByteBuffer serialize(GenericRecord record) {
    return ByteBuffer.wrap(serializer.serialize(record));
  }
}
