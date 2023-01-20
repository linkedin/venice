package com.linkedin.venice.schema.rmd.v1;

import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestCollectionRmdTimestamp {
  private Schema tsSchemaStr;
  private CollectionRmdTimestamp<String> collectionRmdTimestampStr;

  @BeforeMethod
  public void setUp() {
    tsSchemaStr = CollectionRmdTimestamp.createCollectionTimeStampSchema(
        "array_CollectionMetadata_0",
        "com.linkedin.avro",
        Schema.create(Schema.Type.STRING));
    GenericRecord genericRecStr = new GenericData.Record(tsSchemaStr);
    genericRecStr.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    genericRecStr.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 12);
    genericRecStr.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 20);
    genericRecStr.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(21L, 22L, 23L));
    genericRecStr.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(31L, 32L));
    genericRecStr.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("31L", "32L"));
    collectionRmdTimestampStr = new CollectionRmdTimestamp<>(genericRecStr);
  }

  @Test
  public void testCollectionRmdTimestamp() {
    assertEquals(collectionRmdTimestampStr.getTopLevelFieldTimestamp(), 42L);
    assertEquals(collectionRmdTimestampStr.getTopLevelColoID(), 12);
    assertEquals(collectionRmdTimestampStr.getPutOnlyPartLength(), 20);
    assertEquals(collectionRmdTimestampStr.getActiveElementTimestamps(), Arrays.asList(21L, 22L, 23L));
    assertEquals(collectionRmdTimestampStr.getDeletedElementTimestamps(), Arrays.asList(31L, 32L));
    assertEquals(collectionRmdTimestampStr.getDeletedElements(), Arrays.asList("31L", "32L"));
    assertFalse(collectionRmdTimestampStr.isInPutOnlyState());

    CollectionRmdTimestamp<String> collectionRmdTimestamp1 = new CollectionRmdTimestamp<>(collectionRmdTimestampStr);
    assertEquals(collectionRmdTimestamp1, collectionRmdTimestampStr);

    collectionRmdTimestamp1.setActiveElementTimestamps(new ArrayList<>());
    collectionRmdTimestamp1.setDeletedElementsAndTimestamps(new ArrayList<>(), new ArrayList<>());
    assertTrue(collectionRmdTimestamp1.isInPutOnlyState());

    collectionRmdTimestamp1.setPutOnlyPartLength(200);
    collectionRmdTimestamp1.setTopLevelColoID(120);
    collectionRmdTimestamp1.setTopLevelFieldTimestamp(420L);

    assertNotEquals(collectionRmdTimestamp1, collectionRmdTimestampStr);
    assertEquals(new CollectionRmdTimestamp<>(collectionRmdTimestamp1), collectionRmdTimestamp1);
  }

  @Test
  public void testCreateCollectionTimeStampSchema() {
    assertEquals(tsSchemaStr.getField(TOP_LEVEL_TS_FIELD_NAME).schema().getType(), Schema.Type.LONG);
    assertEquals(tsSchemaStr.getField(TOP_LEVEL_COLO_ID_FIELD_NAME).schema().getType(), Schema.Type.INT);
    assertEquals(tsSchemaStr.getField(PUT_ONLY_PART_LENGTH_FIELD_NAME).schema().getType(), Schema.Type.INT);
    assertEquals(tsSchemaStr.getField(ACTIVE_ELEM_TS_FIELD_NAME).schema().getType(), Schema.Type.ARRAY);
    assertEquals(tsSchemaStr.getField(ACTIVE_ELEM_TS_FIELD_NAME).schema().getElementType().getType(), Schema.Type.LONG);
    assertEquals(tsSchemaStr.getField(DELETED_ELEM_FIELD_NAME).schema().getType(), Schema.Type.ARRAY);
    assertEquals(tsSchemaStr.getField(DELETED_ELEM_FIELD_NAME).schema().getElementType().getType(), Schema.Type.STRING);
    assertEquals(tsSchemaStr.getField(DELETED_ELEM_TS_FIELD_NAME).schema().getType(), Schema.Type.ARRAY);
    assertEquals(
        tsSchemaStr.getField(DELETED_ELEM_TS_FIELD_NAME).schema().getElementType().getType(),
        Schema.Type.LONG);
  }

  @Test
  public void testCollectionRmdTimestampEqualsAndHashCode() {
    assertEquals(collectionRmdTimestampStr, collectionRmdTimestampStr);
    assertEquals(collectionRmdTimestampStr.hashCode(), collectionRmdTimestampStr.hashCode());
    assertNotEquals(new Object(), collectionRmdTimestampStr);

    CollectionRmdTimestamp<String> collectionRmdTimestampClone =
        new CollectionRmdTimestamp<>(collectionRmdTimestampStr);
    assertEquals(collectionRmdTimestampStr, collectionRmdTimestampClone);
    assertEquals(collectionRmdTimestampStr.hashCode(), collectionRmdTimestampClone.hashCode());
  }

  @Test
  public void testCollectionRmdTimestampEqualsWithDifferentSchema() {
    Schema timestampSchemaLong = CollectionRmdTimestamp.createCollectionTimeStampSchema(
        "array_CollectionMetadata_0",
        "com.linkedin.avro",
        Schema.create(Schema.Type.LONG));
    GenericRecord genericRecLong = new GenericData.Record(timestampSchemaLong);
    genericRecLong.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    genericRecLong.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 12);
    genericRecLong.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 20);
    genericRecLong.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(21L, 22L, 23L));
    genericRecLong.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(31L, 32L));
    genericRecLong.put(DELETED_ELEM_FIELD_NAME, Arrays.asList(31L, 32L));
    CollectionRmdTimestamp<Integer> collectionRmdTimestampLong = new CollectionRmdTimestamp<>(genericRecLong);
    assertNotEquals(collectionRmdTimestampStr, collectionRmdTimestampLong);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetDeletedElementsAndTimestamps() {
    collectionRmdTimestampStr.setDeletedElementsAndTimestamps(Collections.singletonList("a"), new ArrayList<>());
  }

  @Test
  public void testRemoveDeletionInfoWithTimestampsLowerOrEqualToWhenProvidedTimestampIsPresentInList() {
    collectionRmdTimestampStr.setDeletedElementsAndTimestamps(
        new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")),
        new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L, 100L)));
    collectionRmdTimestampStr.removeDeletionInfoWithTimestampsLowerOrEqualTo(3L);
    assertEquals(collectionRmdTimestampStr.getDeletedElements(), Arrays.asList("e", "f", "g", "h"));
    assertEquals(collectionRmdTimestampStr.getDeletedElementTimestamps(), Arrays.asList(4L, 7L, 7L, 100L));

    collectionRmdTimestampStr.setDeletedElementsAndTimestamps(
        new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")),
        new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L, 100L)));
    collectionRmdTimestampStr.removeDeletionInfoWithTimestampsLowerOrEqualTo(1L);
    assertEquals(collectionRmdTimestampStr.getDeletedElements(), Arrays.asList("b", "c", "d", "e", "f", "g", "h"));
    assertEquals(collectionRmdTimestampStr.getDeletedElementTimestamps(), Arrays.asList(2L, 3L, 3L, 4L, 7L, 7L, 100L));

    collectionRmdTimestampStr.setDeletedElementsAndTimestamps(
        new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")),
        new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L, 100L)));
    collectionRmdTimestampStr.removeDeletionInfoWithTimestampsLowerOrEqualTo(100L);
    assertEquals(collectionRmdTimestampStr.getDeletedElements(), Collections.emptyList());
    assertEquals(collectionRmdTimestampStr.getDeletedElementTimestamps(), Collections.emptyList());
  }

  @Test
  public void testRemoveDeletionInfoWithTimestampsLowerOrEqualToWhenProvidedTimestampIsLowerThanAllTheTimestamps() {
    collectionRmdTimestampStr.setDeletedElementsAndTimestamps(
        new ArrayList<>(Arrays.asList("a", "b")),
        new ArrayList<>(Arrays.asList(4L, 7L)));
    collectionRmdTimestampStr.removeDeletionInfoWithTimestampsLowerOrEqualTo(3);
    assertEquals(collectionRmdTimestampStr.getDeletedElements(), Arrays.asList("a", "b"));
    assertEquals(collectionRmdTimestampStr.getDeletedElementTimestamps(), Arrays.asList(4L, 7L));
  }

  @Test
  public void testRemoveDeletionInfoWithTimestampsLowerOrEqualToWhenProvidedTimestampIsHigherThanAllTheTimestamps() {
    collectionRmdTimestampStr.setDeletedElementsAndTimestamps(
        new ArrayList<>(Arrays.asList("a", "b")),
        new ArrayList<>(Arrays.asList(4L, 7L)));
    collectionRmdTimestampStr.removeDeletionInfoWithTimestampsLowerOrEqualTo(8);
    assertEquals(collectionRmdTimestampStr.getDeletedElements(), Collections.emptyList());
    assertEquals(collectionRmdTimestampStr.getDeletedElementTimestamps(), Collections.emptyList());
  }

  @Test
  public void testRemoveActiveTimestampsLowerOrEqualToWhenProvidedTimestampIsPresentInList() {
    collectionRmdTimestampStr.setActiveElementTimestamps(new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L)));
    collectionRmdTimestampStr.removeActiveTimestampsLowerOrEqualTo(3L);
    assertEquals(collectionRmdTimestampStr.getActiveElementTimestamps(), Arrays.asList(4L, 7L, 7L));

    collectionRmdTimestampStr.setActiveElementTimestamps(new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L)));
    collectionRmdTimestampStr.removeActiveTimestampsLowerOrEqualTo(1L);
    assertEquals(collectionRmdTimestampStr.getActiveElementTimestamps(), Arrays.asList(2L, 3L, 3L, 4L, 7L, 7L));

    collectionRmdTimestampStr.setActiveElementTimestamps(new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L)));
    collectionRmdTimestampStr.removeActiveTimestampsLowerOrEqualTo(7L);
    assertEquals(collectionRmdTimestampStr.getActiveElementTimestamps(), Collections.emptyList());
  }

  @Test
  public void testRemoveActiveTimestampsLowerOrEqualToWhenProvidedTimestampIsLowerThanAllTheTimestamps() {
    collectionRmdTimestampStr.setActiveElementTimestamps(new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L)));
    collectionRmdTimestampStr.removeActiveTimestampsLowerOrEqualTo(0L);
    assertEquals(collectionRmdTimestampStr.getActiveElementTimestamps(), Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L));
  }

  @Test
  public void testRemoveActiveTimestampsLowerOrEqualToWhenProvidedTimestampIsHigherThanAllTheTimestamps() {
    collectionRmdTimestampStr.setActiveElementTimestamps(new ArrayList<>(Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L)));
    collectionRmdTimestampStr.removeActiveTimestampsLowerOrEqualTo(8L);
    assertEquals(collectionRmdTimestampStr.getActiveElementTimestamps(), Collections.emptyList());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*field to be Long type.*")
  public void testValidateCollectionReplicationMetadataRecordShouldHaveTopLevelFieldTimestamp() {
    GenericRecord genericRecLong = new GenericData.Record(tsSchemaStr);
    new CollectionRmdTimestamp<>(genericRecLong);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*field to be Integer type.*")
  public void testValidateCollectionReplicationMetadataRecordShouldHaveTopLevelColoID() {
    GenericRecord genericRecLong = new GenericData.Record(tsSchemaStr);
    genericRecLong.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    new CollectionRmdTimestamp<>(genericRecLong);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*field to be Integer type.*")
  public void testValidateCollectionReplicationMetadataRecordShouldHavePutOnlyPartLength() {
    GenericRecord genericRecLong = new GenericData.Record(tsSchemaStr);
    genericRecLong.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    genericRecLong.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 12);
    new CollectionRmdTimestamp<>(genericRecLong);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*field to be List type.*")
  public void testValidateCollectionReplicationMetadataRecordShouldHaveActiveElementsTimestamps() {
    GenericRecord genericRecLong = new GenericData.Record(tsSchemaStr);
    genericRecLong.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    genericRecLong.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 12);
    genericRecLong.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 20);
    new CollectionRmdTimestamp<>(genericRecLong);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*field to be List type.*")
  public void testValidateCollectionReplicationMetadataRecordShouldHaveDeletedElementsIdentitiesAndTimestamps() {
    GenericRecord genericRecLong = new GenericData.Record(tsSchemaStr);
    genericRecLong.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    genericRecLong.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 12);
    genericRecLong.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 20);
    genericRecLong.put(ACTIVE_ELEM_TS_FIELD_NAME, new ArrayList<>());
    new CollectionRmdTimestamp<>(genericRecLong);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*list should have the same size.*")
  public void testValidateCollectionReplicationMetadataRecordShouldHaveSameSizedIdentitiesAndTimestampsList() {
    GenericRecord genericRecLong = new GenericData.Record(tsSchemaStr);
    genericRecLong.put(TOP_LEVEL_TS_FIELD_NAME, 42L);
    genericRecLong.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 12);
    genericRecLong.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 20);
    genericRecLong.put(ACTIVE_ELEM_TS_FIELD_NAME, new ArrayList<>());
    genericRecLong.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(31L, 32L));
    genericRecLong.put(DELETED_ELEM_FIELD_NAME, Collections.singletonList("32L"));
    new CollectionRmdTimestamp<>(genericRecLong);
  }

  @Test
  public void testFindIndexOfNextLargerNumber() {
    List<Long> list = Arrays.asList(1L, 2L, 3L, 3L, 4L, 7L, 7L, 100L);

    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 1), 1);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 2), 2);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 3), 4);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 4), 5);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 5), 5);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 6), 5);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 7), 7);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 8), 7);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 9), 7);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 60), 7);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 99), 7);

    // Edge cases
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 0), 0);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, Long.MIN_VALUE), 0);
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, 101), list.size());
    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(list, Long.MAX_VALUE), list.size());

    assertEquals(CollectionRmdTimestamp.findIndexOfNextLargerNumber(Collections.emptyList(), 5), 0);
  }
}
