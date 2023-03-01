package com.linkedin.venice.schema.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
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
  /**
   * A schema that contains a list field.
   */
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(
      "{" + "   \"type\" : \"record\"," + "   \"namespace\" : \"com.linkedin.avro\"," + "   \"name\" : \"TestRecord\","
          + "   \"fields\" : ["
          + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"int\"}, \"default\" : [] },"
          + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
          + "   ]" + "}");
  protected static final String LIST_FIELD_NAME = "Items";
  protected static final String MAP_FIELD_NAME = "PetNameToAge";
  private static final Schema RMD_TIMESTAMP_SCHEMA =
      RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA).getField("timestamp").schema().getTypes().get(1);

  @Test
  public void testHandleCollectionMergeMapOp() {
    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder =
        new CollectionTimestampBuilder(Schema.create(Schema.Type.LONG));
    collectionTimestampBuilder.setTopLevelTimestamps(1L);
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(1);

    collectionTimestampBuilder.setActiveElementsTimestamps(Arrays.asList(2L, 3L, 4L));
    collectionTimestampBuilder.setDeletedElementTimestamps(new LinkedList<>());
    collectionTimestampBuilder.setDeletedElements(Schema.create(Schema.Type.LONG), new LinkedList<>());
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
    List<String> toRemoveKeys = new LinkedList<>();
    handlerToTest.handleModifyMap(3L, collectionMetadata, currValueRecord, MAP_FIELD_NAME, newEntries, toRemoveKeys);

    Map<String, Integer> updatedMap = (Map<String, Integer>) currValueRecord.get(MAP_FIELD_NAME);
    Map<String, Integer> expectedMap = new HashMap<>();
    expectedMap.put("key1", 2);
    expectedMap.put("key2", 2);
    expectedMap.put("key3", 2);
    expectedMap.put("key4", 1);
    Assert.assertEquals(updatedMap, expectedMap);
  }
}
