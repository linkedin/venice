package com.linkedin.venice.writer.update;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.ddsstorage.io.IOUtils;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpdateBuilderImplTest {

  private static final Logger logger = LogManager.getLogger(UpdateBuilderImplTest.class);
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(loadFileAsString("TestWriteComputeBuilder.avsc"));
  private static final Schema UPDATE_SCHEMA = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA).getTypes().get(0);

  @Test
  public void testUpdateWholeField() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    String expectedName = "Lebron James";
    GenericRecord expectedAddress = createAddressRecord("222 2nd street", "San Francisco");
    int expectedAge = 36;
    List<Integer> expectedIntArray = Arrays.asList(1, 3, 5, 7);
    Map<String, String> expectedStringMap = new LinkedHashMap<>();
    expectedStringMap.put("1", "one");
    expectedStringMap.put("2", "two");
    expectedStringMap.put("3", "three");

    builder.setNewFieldValue("name", expectedName);
    builder.setNewFieldValue("age", expectedAge);
    builder.setNewFieldValue("intArray", expectedIntArray);
    builder.setNewFieldValue("stringMap", expectedStringMap);
    builder.setNewFieldValue("address", expectedAddress);

    GenericRecord updateRecord = builder.build();

    Assert.assertEquals(updateRecord.get("name"), expectedName);
    Assert.assertEquals(updateRecord.get("age"), expectedAge);
    Assert.assertEquals(updateRecord.get("intArray"), expectedIntArray);
    Assert.assertEquals(updateRecord.get("stringMap"), expectedStringMap);
    Assert.assertEquals(updateRecord.get("address"), expectedAddress);
    Assert.assertEquals(updateRecord.get("recordMap"), createFieldNoOpRecord("recordMap"));
    Assert.assertEquals(updateRecord.get("recordArray"), createFieldNoOpRecord("recordArray"));
  }

  @Test
  public void testSetOneFieldMultipleTimes() {
    String expectedName = "Lebron James";
    int expectedAge = 30;
    GenericRecord expectedAddress = createAddressRecord("street 2", "San Francisco");
    List<Integer> expectedIntArray = Arrays.asList(4, 5, 6);
    Map<String, String> expectedStringMap = new LinkedHashMap<>();
    expectedStringMap.put("1", "one");
    expectedStringMap.put("2", "two");
    expectedStringMap.put("3", "three");
    Map<String, String> tmpStringMap = new LinkedHashMap<>();
    expectedStringMap.put("5", "five");
    expectedStringMap.put("6", "six");

    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);

    builder.setNewFieldValue("name", "Kobe");
    builder.setNewFieldValue("name", expectedName); // This value should override the previous value.

    builder.setNewFieldValue("age", 29);
    builder.setNewFieldValue("age", expectedAge); // This value should override the previous value.

    builder.setNewFieldValue("address", createAddressRecord("street 1", "Seattle"));
    builder.setNewFieldValue("address", expectedAddress);

    builder.setNewFieldValue("intArray", Arrays.asList(1, 2, 3));
    builder.setNewFieldValue("intArray", expectedIntArray);

    builder.setNewFieldValue("stringMap", tmpStringMap);
    builder.setNewFieldValue("stringMap", expectedStringMap);

    GenericRecord updateRecord = builder.build();
    Assert.assertEquals(updateRecord.get("name"), expectedName);
    Assert.assertEquals(updateRecord.get("age"), expectedAge);
    Assert.assertEquals(updateRecord.get("intArray"), expectedIntArray);
    Assert.assertEquals(updateRecord.get("stringMap"), expectedStringMap);
    Assert.assertEquals(updateRecord.get("address"), expectedAddress);
    Assert.assertEquals(updateRecord.get("recordMap"), createFieldNoOpRecord("recordMap"));
    Assert.assertEquals(updateRecord.get("recordArray"), createFieldNoOpRecord("recordArray"));
  }

  @Test
  public void testCreateCollectionMergeRequest() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    List<Integer> expectedIntArrayToAdd = Arrays.asList(1, 3, 5, 7);
    List<Integer> expectedIntArrayToRemove = Arrays.asList(2, 4, 6, 8);
    Map<String, String> expectedStringMapToAdd = new LinkedHashMap<>();
    expectedStringMapToAdd.put("1", "one");
    expectedStringMapToAdd.put("2", "two");
    expectedStringMapToAdd.put("3", "three");
    List<String> expectedStringMapKeysToRemove = Arrays.asList("4", "5", "6");

    builder.setElementsToAddToListField("intArray", expectedIntArrayToAdd);
    builder.setElementsToRemoveFromListField("intArray", expectedIntArrayToRemove);
    builder.setEntriesToAddToMapField("stringMap", expectedStringMapToAdd);
    builder.setKeysToRemoveFromMapField("stringMap", expectedStringMapKeysToRemove);

    GenericRecord updateRecord = builder.build();
    System.out.println(updateRecord);

    // Fields with no change (no-op).
    Assert.assertEquals(updateRecord.get("name"), createFieldNoOpRecord("name"));
    Assert.assertEquals(updateRecord.get("age"), createFieldNoOpRecord("age"));
    Assert.assertEquals(updateRecord.get("address"), createFieldNoOpRecord("address"));
    Assert.assertEquals(updateRecord.get("recordMap"), createFieldNoOpRecord("recordMap"));
    Assert.assertEquals(updateRecord.get("recordArray"), createFieldNoOpRecord("recordArray"));

    // Fields where collection merge happens.
    Assert.assertTrue(updateRecord.get("intArray") instanceof GenericRecord);
    GenericRecord listMergeRecord = (GenericRecord) updateRecord.get("intArray");
    Assert.assertEquals(listMergeRecord.get("setUnion"), expectedIntArrayToAdd);
    Assert.assertEquals(listMergeRecord.get("setDiff"), expectedIntArrayToRemove);

    Assert.assertTrue(updateRecord.get("stringMap") instanceof GenericRecord);
    GenericRecord mapMergeRecord = (GenericRecord) updateRecord.get("stringMap");
    Assert.assertEquals(mapMergeRecord.get("mapUnion"), expectedStringMapToAdd);
    Assert.assertEquals(mapMergeRecord.get("mapDiff"), expectedStringMapKeysToRemove);
  }

  @Test
  public void testBuildWithNoUpdate() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    Assert.assertThrows(
        IllegalStateException.class,
        builder::build
    );
  }

  /**
   * A collection field should be either NO_OP, given a new value, or collection merge value. This test covers the situation
   * where a collection field was given both a new value but collection merge still happens afterwards.
   */
  @Test
  public void testDuplicatedSetCollectionMerge() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    List<Integer> expectedIntArray = Arrays.asList(1, 3, 5, 7);
    Map<String, String> expectedStringMap = new LinkedHashMap<>();
    expectedStringMap.put("1", "one");
    expectedStringMap.put("2", "two");
    expectedStringMap.put("3", "three");

    builder.setNewFieldValue("intArray", expectedIntArray);
    builder.setNewFieldValue("stringMap", expectedStringMap);

    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setElementsToAddToListField("intArray", Arrays.asList(5, 6, 7))
    );
    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setElementsToRemoveFromListField("intArray", Arrays.asList(1, 2))
    );

    Map<String, String> mapEntriesToAdd = new LinkedHashMap<>();
    expectedStringMap.put("3", "three");
    expectedStringMap.put("4", "four");
    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setEntriesToAddToMapField("stringMap", mapEntriesToAdd)
    );
    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setKeysToRemoveFromMapField("stringMap", Arrays.asList("3", "4"))
    );
  }

  /**
   * A collection field should be either NO_OP, given a new value, or collection merge value. This test covers the situation
   * where a collection field is given a collection merge but it is still set to a whole new collection value afterwards.
   */
  @Test
  public void testDuplicatedSetCollectionValue() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);

    Map<String, String> mapEntriesToAdd = new LinkedHashMap<>();
    mapEntriesToAdd.put("3", "three");
    mapEntriesToAdd.put("4", "four");

    builder.setEntriesToAddToMapField("stringMap", mapEntriesToAdd);
    builder.setKeysToRemoveFromMapField("stringMap", Arrays.asList("1", "2"));

    builder.setElementsToAddToListField("intArray", Arrays.asList(5, 6, 7));
    builder.setElementsToRemoveFromListField("intArray", Arrays.asList(1, 2));

    List<Integer> expectedIntArray = Arrays.asList(1, 3, 5, 7);
    Map<String, String> expectedStringMap = new LinkedHashMap<>();
    expectedStringMap.put("1", "one");
    expectedStringMap.put("2", "two");

    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setNewFieldValue("intArray", expectedIntArray)
    );

    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setNewFieldValue("stringMap", expectedStringMap)
    );
  }

  private GenericRecord createAddressRecord(String street, String city) {
    Schema addressRecordSchema = VALUE_SCHEMA.getField("address").schema().getTypes().get(1);
    GenericRecord addressRecord = new GenericData.Record(addressRecordSchema);
    addressRecord.put("streetaddress", street);
    addressRecord.put("city", city);
    return addressRecord;
  }

  private GenericRecord createFieldNoOpRecord(String fieldName) {
    Schema noOpSchema = UPDATE_SCHEMA.getField(fieldName).schema().getTypes().get(0);
    return new GenericData.Record(noOpSchema);
  }

  private static String loadFileAsString(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8
      );
    } catch (Exception e) {
      logger.error(e);
      return null;
    }
  }
}
