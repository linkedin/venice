package com.linkedin.venice.schema.merge;

import static com.linkedin.venice.schema.Utils.loadSchemaFileAsString;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CollectionMergeTest {
  private static final Schema VALUE_SCHEMA =
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadSchemaFileAsString("testMergeSchema.avsc"));
  protected static final String LIST_FIELD_NAME = "StringListField";
  protected static final String MAP_FIELD_NAME = "NullableIntMapField";
  private static final Schema RMD_TIMESTAMP_SCHEMA = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA)
      .getField(RmdConstants.TIMESTAMP_FIELD_NAME)
      .schema()
      .getTypes()
      .get(1);

  @Test
  public void testHandleCollectionMergeMapOp() {
    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder =
        new CollectionTimestampBuilder(Schema.create(Schema.Type.LONG));
    collectionTimestampBuilder.setTopLevelTimestamps(1L);
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(1);

    collectionTimestampBuilder.setActiveElementsTimestamps(Arrays.asList(2L, 3L, 4L));
    collectionTimestampBuilder.setDeletedElementTimestamps(Arrays.asList(2L, 3L, 4L));
    collectionTimestampBuilder
        .setDeletedElements(Schema.create(Schema.Type.STRING), Arrays.asList("key5", "key6", "key7"));
    collectionTimestampBuilder.setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(MAP_FIELD_NAME).schema());
    CollectionRmdTimestamp<String> collectionMetadata =
        new CollectionRmdTimestamp<>(collectionTimestampBuilder.build());
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    // Test not-put-only state.
    IndexedHashMap<String, Integer> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", 1);
    mapValue.put("key2", 1);
    mapValue.put("key3", 1);
    mapValue.put("key4", 1);
    currValueRecord.put(MAP_FIELD_NAME, mapValue);

    Map<String, Object> newEntries = new HashMap<>();
    newEntries.put("key1", 2);
    newEntries.put("key2", 2);
    newEntries.put("key3", 2);
    newEntries.put("key4", 2);
    newEntries.put("key5", 2);
    newEntries.put("key6", 2);
    newEntries.put("key7", 2);
    List<String> toRemoveKeys = new LinkedList<>();
    handlerToTest.handleModifyMap(3L, collectionMetadata, currValueRecord, MAP_FIELD_NAME, newEntries, toRemoveKeys);

    Map<String, Integer> updatedMap = (Map<String, Integer>) currValueRecord.get(MAP_FIELD_NAME);
    Map<String, Integer> expectedMap = new HashMap<>();
    expectedMap.put("key1", 2);
    expectedMap.put("key2", 2);
    expectedMap.put("key3", 2);
    expectedMap.put("key4", 1);
    expectedMap.put("key5", 2);
    Assert.assertEquals(updatedMap, expectedMap);
  }

  @Test
  public void testHandleCollectionMergeListOp() {
    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder =
        new CollectionTimestampBuilder(Schema.create(Schema.Type.STRING));
    collectionTimestampBuilder.setTopLevelTimestamps(1L);
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(1);
    collectionTimestampBuilder.setActiveElementsTimestamps(Arrays.asList(2L, 3L));
    collectionTimestampBuilder.setDeletedElementTimestamps(Arrays.asList(1L, 2L, 3L));
    collectionTimestampBuilder
        .setDeletedElements(Schema.create(Schema.Type.STRING), Arrays.asList("key4", "key5", "key6"));
    collectionTimestampBuilder.setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(LIST_FIELD_NAME).schema());
    CollectionRmdTimestamp<Object> collectionMetadata =
        new CollectionRmdTimestamp<>(collectionTimestampBuilder.build());
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    // Test not-put-only state.
    currValueRecord.put(LIST_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));

    List<Object> toAddItems = Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6");
    List<Object> toRemoveItems = new LinkedList<>();
    handlerToTest.handleModifyList(2L, collectionMetadata, currValueRecord, LIST_FIELD_NAME, toAddItems, toRemoveItems);

    List<String> updatedMap = (List<String>) currValueRecord.get(LIST_FIELD_NAME);
    Assert.assertEquals(updatedMap, Arrays.asList("key1", "key2", "key3", "key4"));
  }

}
