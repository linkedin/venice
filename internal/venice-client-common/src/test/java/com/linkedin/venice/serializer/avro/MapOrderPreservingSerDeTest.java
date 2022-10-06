package com.linkedin.venice.serializer.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MapOrderPreservingSerDeTest {
  /**
   * A schema that contains primitive fields and collection fields, specifically, a list field and a map field.
   */
  private static final String VALUE_SCHEMA_STR = "{" + "   \"type\" : \"record\","
      + "   \"namespace\" : \"com.linkedin.avro\"," + "   \"name\" : \"Person\"," + "   \"fields\" : ["
      + "      { \"name\" : \"Name\" , \"type\" : \"string\", \"default\" : \"unknown\" },"
      + "      { \"name\" : \"Age\" , \"type\" : \"int\", \"default\" : -1 },"
      + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"string\"}, \"default\" : [] },"
      + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
      + "   ]" + "}";

  @Test
  public void testCollectionFieldsDeserializedInConsistentOrder() {
    Schema valueSchema = AvroCompatibilityHelper.parse(VALUE_SCHEMA_STR);
    MapOrderPreservingSerializer<GenericRecord> serializer =
        MapOrderingPreservingSerDeFactory.getSerializer(valueSchema);
    MapOrderPreservingDeserializer deserializer =
        MapOrderingPreservingSerDeFactory.getDeserializer(valueSchema, valueSchema);

    List<Pair<String, Integer>> mapEntries = Arrays.asList(
        new Pair<>("cat_1", 1),
        new Pair<>("cat_2", 2),
        new Pair<>("cat_3", 3),
        new Pair<>("cat_4", 4),
        new Pair<>("cat_5", 5),
        new Pair<>("cat_6", 6),
        new Pair<>("cat_7", 7),
        new Pair<>("cat_8", 8),
        new Pair<>("cat_9", 9));
    List<String> listEntries = Arrays.asList("1", "2", "3", "4", "5", "5");

    validateConsistentSerdeResults(listEntries, mapEntries, serializer, deserializer, valueSchema, 100);
  }

  @Test
  public void testDeserializeEmptyCollectionFields() {
    Schema valueSchema = AvroCompatibilityHelper.parse(VALUE_SCHEMA_STR);
    MapOrderPreservingSerializer<GenericRecord> serializer =
        MapOrderingPreservingSerDeFactory.getSerializer(valueSchema);
    MapOrderPreservingDeserializer deserializer = new MapOrderPreservingDeserializer(valueSchema, valueSchema);

    validateConsistentSerdeResults(
        Collections.emptyList(),
        Collections.emptyList(),
        serializer,
        deserializer,
        valueSchema,
        1);
  }

  /**
   * This method takes a list and a map and shuffle them and then serde them to make sure that the deserialized list and
   * map still have the same ordering of elements before serialization. It repeats this process as specified.
   */
  private void validateConsistentSerdeResults(
      List<String> listEntries,
      List<Pair<String, Integer>> mapEntries,
      MapOrderPreservingSerializer<GenericRecord> serializer,
      MapOrderPreservingDeserializer deserializer,
      Schema valueSchema,
      final int totalShuffleCount) {
    for (int i = 0; i < totalShuffleCount; i++) {
      GenericRecord valueRecord = new GenericData.Record(valueSchema);
      valueRecord.put("Name", "Alice");
      valueRecord.put("Age", 23);
      valueRecord.put("Items", listEntries);

      Collections.shuffle(mapEntries);
      Collections.shuffle(listEntries);
      valueRecord.put("Items", listEntries);

      Map<String, Integer> petNameToAge = new IndexedHashMap<>();
      for (Pair<String, Integer> mapEntry: mapEntries) {
        petNameToAge.put(mapEntry.getFirst(), mapEntry.getSecond());
      }
      valueRecord.put("PetNameToAge", petNameToAge);

      byte[] serializedBytes = serializer.serialize(valueRecord);
      GenericRecord deserializedRecord = deserializer.deserialize(serializedBytes);

      IndexedHashMap<Utf8, Integer> deserializedMap =
          (IndexedHashMap<Utf8, Integer>) deserializedRecord.get("PetNameToAge");
      List<Pair<String, Integer>> deserializedMapEntries = new ArrayList<>(deserializedMap.size());
      for (int j = 0; j < deserializedMap.size(); j++) {
        Map.Entry<Utf8, Integer> mapEntry = deserializedMap.getByIndex(j);
        deserializedMapEntries.add(new Pair<>(mapEntry.getKey().toString(), mapEntry.getValue()));
      }
      List<String> deserializedListEntries =
          ((List<Utf8>) deserializedRecord.get("Items")).stream().map(Utf8::toString).collect(Collectors.toList());

      Assert.assertEquals(mapEntries, deserializedMapEntries);
      Assert.assertEquals(mapEntries, deserializedMapEntries);
      Assert.assertEquals(listEntries, deserializedListEntries);
    }
  }
}
