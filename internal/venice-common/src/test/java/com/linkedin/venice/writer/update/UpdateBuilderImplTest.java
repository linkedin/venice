package com.linkedin.venice.writer.update;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpdateBuilderImplTest {
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(TestUtils.loadFileAsString("PersonV1.avsc"));
  private static final Schema EVOLVED_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(TestUtils.loadFileAsString("PersonV2.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);
  private static final Schema EVOLVED_UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(EVOLVED_VALUE_SCHEMA);

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
  public void testUpdateEvolvedSubfieldRemoveFieldsNotInUpdateSchema() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    String expectedName = "Lebron James";
    GenericRecord writeAddress = createEvolvedAddressRecord("222 2nd street", "San Francisco", "CA");
    GenericRecord expectedAddress = createAddressRecord("222 2nd street", "San Francisco");
    int expectedAge = 36;
    List<Integer> expectedIntArray = Arrays.asList(1, 3, 5, 7);

    List<GenericRecord> writeRecordArray = new ArrayList<>();
    writeRecordArray.add(createEvolvedRecordForListField(1, "testName"));

    List<GenericRecord> expectedRecordArray = new ArrayList<>();
    expectedRecordArray.add(createRecordForListField(1));

    Map<String, String> expectedStringMap = new LinkedHashMap<>();
    expectedStringMap.put("1", "one");
    expectedStringMap.put("2", "two");
    expectedStringMap.put("3", "three");

    Map<String, GenericRecord> writeRecordMapToAdd = new LinkedHashMap<>();
    writeRecordMapToAdd.put("1", createEvolvedMapEntry(1, "firstName"));
    writeRecordMapToAdd.put("2", createEvolvedMapEntry(2, "secondName"));

    Map<String, GenericRecord> expectedRecordMapToAdd = new LinkedHashMap<>();
    expectedRecordMapToAdd.put("1", createMapEntry(1));
    expectedRecordMapToAdd.put("2", createMapEntry(2));

    builder.setNewFieldValue("name", expectedName);
    builder.setNewFieldValue("age", expectedAge);
    builder.setNewFieldValue("intArray", expectedIntArray);
    builder.setElementsToAddToListField("recordArray", writeRecordArray);
    builder.setNewFieldValue("stringMap", expectedStringMap);
    builder.setNewFieldValue("address", writeAddress);
    builder.setEntriesToAddToMapField("recordMap", writeRecordMapToAdd);

    GenericRecord updateRecord = builder.build();

    Assert.assertEquals(updateRecord.get("name"), expectedName);
    Assert.assertEquals(updateRecord.get("age"), expectedAge);
    Assert.assertEquals(updateRecord.get("intArray"), expectedIntArray);
    Assert.assertEquals(updateRecord.get("stringMap"), expectedStringMap);
    Assert.assertEquals(updateRecord.get("address"), expectedAddress);

    Assert.assertTrue(updateRecord.get("recordArray") instanceof GenericRecord);
    GenericRecord listMergeRecord = (GenericRecord) updateRecord.get("recordArray");
    Assert.assertEquals(listMergeRecord.get("setUnion"), expectedRecordArray);
    Assert.assertEquals(listMergeRecord.get("setDiff"), Collections.emptyList());

    Assert.assertTrue(updateRecord.get("recordMap") instanceof GenericRecord);
    GenericRecord recordMapMergeRecord = (GenericRecord) updateRecord.get("recordMap");
    Assert.assertEquals(recordMapMergeRecord.get("mapUnion"), expectedRecordMapToAdd);
    Assert.assertEquals(recordMapMergeRecord.get("mapDiff"), Collections.emptyList());
  }

  @Test
  public void testUpdateEvolvedSubfieldFillDefaultsForUnspecifiedFields() {
    UpdateBuilder builder = new UpdateBuilderImpl(EVOLVED_UPDATE_SCHEMA);
    String expectedName = "Lebron James";
    GenericRecord writeAddress = createAddressRecord("222 2nd street", "San Francisco");
    GenericRecord expectedAddress = createEvolvedAddressRecord("222 2nd street", "San Francisco", "California");
    int expectedAge = 36;
    List<Integer> expectedIntArray = Arrays.asList(1, 3, 5, 7);

    List<GenericRecord> writeRecordArray = new ArrayList<>();
    writeRecordArray.add(createRecordForListField(1));

    List<GenericRecord> expectedRecordArray = new ArrayList<>();
    expectedRecordArray.add(createEvolvedRecordForListField(1, "venice"));

    Map<String, String> expectedStringMap = new LinkedHashMap<>();
    expectedStringMap.put("1", "one");
    expectedStringMap.put("2", "two");
    expectedStringMap.put("3", "three");

    Map<String, GenericRecord> writeRecordMapToAdd = new LinkedHashMap<>();
    writeRecordMapToAdd.put("1", createMapEntry(1));
    writeRecordMapToAdd.put("2", createMapEntry(2));

    Map<String, GenericRecord> expectedRecordMapToAdd = new LinkedHashMap<>();
    expectedRecordMapToAdd.put("1", createEvolvedMapEntry(1, "venice"));
    expectedRecordMapToAdd.put("2", createEvolvedMapEntry(2, "venice"));

    builder.setNewFieldValue("name", expectedName);
    builder.setNewFieldValue("age", expectedAge);
    builder.setNewFieldValue("intArray", expectedIntArray);
    builder.setElementsToAddToListField("recordArray", writeRecordArray);
    builder.setNewFieldValue("stringMap", expectedStringMap);
    builder.setNewFieldValue("address", writeAddress);
    builder.setEntriesToAddToMapField("recordMap", writeRecordMapToAdd);

    GenericRecord updateRecord = builder.build();

    Assert.assertEquals(updateRecord.get("name"), expectedName);
    Assert.assertEquals(updateRecord.get("age"), expectedAge);
    Assert.assertEquals(updateRecord.get("intArray"), expectedIntArray);
    Assert.assertEquals(updateRecord.get("stringMap"), expectedStringMap);
    Assert.assertEquals(updateRecord.get("address"), expectedAddress);

    Assert.assertTrue(updateRecord.get("recordArray") instanceof GenericRecord);
    GenericRecord listMergeRecord = (GenericRecord) updateRecord.get("recordArray");
    Assert.assertEquals(listMergeRecord.get("setUnion"), expectedRecordArray);
    Assert.assertEquals(listMergeRecord.get("setDiff"), Collections.emptyList());

    Assert.assertTrue(updateRecord.get("recordMap") instanceof GenericRecord);
    GenericRecord recordMapMergeRecord = (GenericRecord) updateRecord.get("recordMap");
    Assert.assertEquals(recordMapMergeRecord.get("mapUnion"), expectedRecordMapToAdd);
    Assert.assertEquals(recordMapMergeRecord.get("mapDiff"), Collections.emptyList());
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

    Map<String, GenericRecord> expectedRecordMapToAdd = new LinkedHashMap<>();
    expectedRecordMapToAdd.put("1", createMapEntry(1));
    expectedRecordMapToAdd.put("2", createMapEntry(2));

    List<String> expectedRecordMapKeysToRemove = Arrays.asList("4", "5", "6");

    builder.setElementsToAddToListField("intArray", expectedIntArrayToAdd);
    builder.setElementsToRemoveFromListField("intArray", expectedIntArrayToRemove);
    builder.setEntriesToAddToMapField("stringMap", expectedStringMapToAdd);
    builder.setKeysToRemoveFromMapField("stringMap", expectedStringMapKeysToRemove);
    builder.setEntriesToAddToMapField("recordMap", expectedRecordMapToAdd);
    builder.setKeysToRemoveFromMapField("recordMap", expectedRecordMapKeysToRemove);

    GenericRecord updateRecord = builder.build();

    // Fields with no change (no-op).
    Assert.assertEquals(updateRecord.get("name"), createFieldNoOpRecord("name"));
    Assert.assertEquals(updateRecord.get("age"), createFieldNoOpRecord("age"));
    Assert.assertEquals(updateRecord.get("address"), createFieldNoOpRecord("address"));
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

    Assert.assertTrue(updateRecord.get("recordMap") instanceof GenericRecord);
    GenericRecord recordMapMergeRecord = (GenericRecord) updateRecord.get("recordMap");
    Assert.assertEquals(recordMapMergeRecord.get("mapUnion"), expectedRecordMapToAdd);
    Assert.assertEquals(recordMapMergeRecord.get("mapDiff"), expectedRecordMapKeysToRemove);
  }

  @Test
  public void testBuildWithNoUpdate() {
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    Assert.assertThrows(IllegalStateException.class, builder::build);
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
        () -> builder.setElementsToAddToListField("intArray", Arrays.asList(5, 6, 7)));
    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setElementsToRemoveFromListField("intArray", Arrays.asList(1, 2)));

    Map<String, String> mapEntriesToAdd = new LinkedHashMap<>();
    expectedStringMap.put("3", "three");
    expectedStringMap.put("4", "four");
    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setEntriesToAddToMapField("stringMap", mapEntriesToAdd));
    Assert.assertThrows(
        IllegalStateException.class,
        () -> builder.setKeysToRemoveFromMapField("stringMap", Arrays.asList("3", "4")));
  }

  /**
   * A collection field should be either NO_OP, given a new value, or collection merge value. This test covers the situation
   * where a collection field is given a collection merge, but it is still set to a whole new collection value afterwards.
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

    Assert.assertThrows(IllegalStateException.class, () -> builder.setNewFieldValue("intArray", expectedIntArray));

    Assert.assertThrows(IllegalStateException.class, () -> builder.setNewFieldValue("stringMap", expectedStringMap));
  }

  /**
   * User of the {@link UpdateBuilder} should be able to set field to null when the field is of type union and has null
   * in one of the union branches.
   */
  @Test
  public void testSetFieldToNull() {
    // Allow nullable field to be updated to null.
    UpdateBuilder builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    builder.setNewFieldValue("address", null);
    GenericRecord partialUpdateRecord = builder.build();
    Assert.assertNull(partialUpdateRecord.get("address"));

    // It should throw exception when non-nullable field is set to null.
    UpdateBuilder builder2 = new UpdateBuilderImpl(UPDATE_SCHEMA);
    Assert.assertThrows(VeniceException.class, () -> builder2.setNewFieldValue("name", null));
  }

  /**
   * Should return a VeniceException with more information on the unresolved datum type when field is of type union
   * and the value doesn't match one of the union branches.
   */
  @Test
  public void testValidateUpdateRecordIsSerializable() {
    UpdateBuilderImpl builder = new UpdateBuilderImpl(UPDATE_SCHEMA);
    VeniceAvroKafkaSerializer serializer = new VeniceAvroKafkaSerializer(UPDATE_SCHEMA);
    String topic = "dummyTopic";

    // All good values
    GenericRecord record = generateValidRecord();
    Exception e = builder.validateUpdateRecordIsSerializable(record);
    assertNull(e);
    byte[] goodBytes = serializer.serialize(topic, record);

    // The "name" field is mandatory, so not setting it will leave it null, which is wrong.
    record = new GenericData.Record(UPDATE_SCHEMA);
    e = builder.validateUpdateRecordIsSerializable(record);
    assertTrue(e instanceof VeniceSerializationException);
    assertEquals(e.getMessage(), "The following type does not conform to any branch of the union: null");

    // The "name" field is of type string, so a boolean value is also wrong
    record.put("name", true);
    e = builder.validateUpdateRecordIsSerializable(record);
    assertTrue(e instanceof VeniceSerializationException);
    assertEquals(e.getMessage(), "The following type does not conform to any branch of the union: Boolean");

    // Almost correct record, but with wrong namespace
    record = generateValidRecord();
    List<Schema.Field> fields = new ArrayList<>(2);
    fields.add(
        AvroCompatibilityHelper.createSchemaField("streetaddress", Schema.create(Schema.Type.STRING), "", "unknown"));
    fields.add(AvroCompatibilityHelper.createSchemaField("city", Schema.create(Schema.Type.STRING), "", "Sunnyvale"));
    Schema wrongAddressSchema = Schema.createRecord("AddressUSRecord", "", "wrong.namespace", false, fields);
    GenericRecord wrongAddressRecord = new GenericData.Record(wrongAddressSchema);
    record.put("address", wrongAddressRecord);
    e = builder.validateUpdateRecordIsSerializable(record);
    assertTrue(e instanceof VeniceSerializationException);
    assertEquals(
        e.getMessage(),
        "The following type does not conform to any branch of the union: {\"type\":\"record\",\"name\":\"AddressUSRecord\",\"namespace\":\"wrong.namespace\",\"doc\":\"\",\"fields\":[{\"name\":\"streetaddress\",\"type\":\"string\",\"doc\":\"\",\"default\":\"unknown\"},{\"name\":\"city\",\"type\":\"string\",\"doc\":\"\",\"default\":\"Sunnyvale\"}]}");

    // After exercising the serializer to detect invalid records, the thread local internal state should not be corrupt
    record = generateValidRecord();
    byte[] bytes = serializer.serialize(topic, record);
    assertEquals(
        bytes,
        goodBytes,
        "The binary payload produced by the serializer after having checked bad records is not the same as in the beginning!");

    // The deserialization below would throw if we passed badly serialized payload due to corrupt internal state.
    GenericRecord genericRecordResult = (GenericRecord) serializer.deserialize(topic, bytes);
    assertEquals(genericRecordResult, record, "Serialization and deserialization should yield equal records.");
  }

  private GenericRecord generateValidRecord() {
    GenericRecord record = new GenericData.Record(UPDATE_SCHEMA);
    // All good values
    record.put("name", "John Doe");
    record.put("age", 60);
    // The "address" field can be omitted since it allows null...
    record.put("intArray", Collections.emptyList());
    record.put("recordArray", Collections.emptyList());
    record.put("stringMap", Collections.emptyMap());
    record.put("recordMap", Collections.emptyMap());
    return record;
  }

  private GenericRecord createRecordForListField(int number) {
    Schema recordSchema = VALUE_SCHEMA.getField("recordArray").schema().getElementType();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("number", number);
    return record;
  }

  private GenericRecord createEvolvedRecordForListField(int number, String name) {
    Schema recordSchema = EVOLVED_VALUE_SCHEMA.getField("recordArray").schema().getElementType();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("number", number);
    record.put("name", name);
    return record;
  }

  private GenericRecord createAddressRecord(String street, String city) {
    Schema addressRecordSchema = VALUE_SCHEMA.getField("address").schema().getTypes().get(1);
    GenericRecord addressRecord = new GenericData.Record(addressRecordSchema);
    addressRecord.put("streetaddress", street);
    addressRecord.put("city", city);
    return addressRecord;
  }

  private GenericRecord createEvolvedAddressRecord(String street, String city, String state) {
    Schema addressRecordSchema = EVOLVED_VALUE_SCHEMA.getField("address").schema().getTypes().get(1);
    GenericRecord addressRecord = new GenericData.Record(addressRecordSchema);
    addressRecord.put("streetaddress", street);
    addressRecord.put("city", city);
    addressRecord.put("state", state);
    return addressRecord;
  }

  private GenericRecord createMapEntry(int number) {
    Schema recordSchema = VALUE_SCHEMA.getField("recordMap").schema().getValueType();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("number", number);
    return record;
  }

  private GenericRecord createEvolvedMapEntry(int number, String name) {
    Schema recordSchema = EVOLVED_VALUE_SCHEMA.getField("recordMap").schema().getValueType();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("number", number);
    record.put("name", name);
    return record;
  }

  private GenericRecord createFieldNoOpRecord(String fieldName) {
    Schema noOpSchema = UPDATE_SCHEMA.getField(fieldName).schema().getTypes().get(0);
    return new GenericData.Record(noOpSchema);
  }
}
