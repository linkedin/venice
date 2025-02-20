package org.apache.avro.specific;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Encoder;
import org.testng.annotations.Test;


public class WriterSpecificDataTest {
  @Test
  public void testGetField() {
    String schemaString =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"fieldA\",\"type\":\"string\"},{\"name\":\"fieldB\",\"type\":\"int\"},{\"name\":\"fieldC\",\"type\":\"boolean\"}]}";
    Schema schema = new Schema.Parser().parse(schemaString);

    // Create the record
    GenericRecord record = new GenericData.Record(schema);
    record.put("fieldA", "valueA");
    record.put("fieldB", 123);
    record.put("fieldC", true);

    // Create the WriterSpecificData object
    WriterSpecificData writerSpecificData = new WriterSpecificData();

    // Get the value of the field by name, providing the wrong index
    Object fieldA = writerSpecificData.getField(record, "fieldA", 1);
    assertEquals(fieldA, "valueA");
    Object fieldB = writerSpecificData.getField(record, "fieldB", 0);
    assertEquals(fieldB, 123);

    // Get the value of field by name, providing the correct index
    Object fieldC = writerSpecificData.getField(record, "fieldC", 2);
    assertEquals(fieldC, true);

    // Test getField method with non-existing field
    try {
      writerSpecificData.getField(record, "nonExistingField", 0);
    } catch (IllegalArgumentException exception) {
      assertEquals("Field nonExistingField not found in schema.", exception.getMessage());
    } catch (Exception exception) {
      assertThrows(IllegalArgumentException.class, () -> {
        throw exception;
      });
    }
  }

  @Test
  public void testSerialization() throws IOException {
    String schemaString =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"fieldA\",\"type\":\"string\"},{\"name\":\"fieldB\",\"type\":\"int\"},{\"name\":\"fieldC\",\"type\":\"boolean\"}]}";
    Schema oldSchema = new Schema.Parser().parse(schemaString);

    // New Schema with fieldD in the middle
    String readerSchemaString =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"fieldA\",\"type\":\"string\"},{\"name\":\"fieldD\",\"type\":\"boolean\"},{\"name\":\"fieldB\",\"type\":\"int\"},{\"name\":\"fieldC\",\"type\":\"boolean\"}]}";
    Schema newSchema = new Schema.Parser().parse(readerSchemaString);

    // Create the record from new schema
    GenericRecord record = new GenericData.Record(newSchema);
    record.put("fieldA", "valueA");
    record.put("fieldD", false);
    record.put("fieldB", 123);
    record.put("fieldC", true);

    // Serialize the record
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, true, null);

    // Use WriterSpecificData to serialize the record
    // The test will fail if not using WriterSpecificData with java.lang.ClassCastException error
    SpecificDatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(oldSchema, new WriterSpecificData());

    writer.write(record, encoder);
    encoder.flush();
    byte[] serializedRecord = byteArrayOutputStream.toByteArray();
    // Deserialize the record
    SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<>(oldSchema, oldSchema);
    InputStream in = new ByteArrayInputStream(serializedRecord);
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, true, null);
    GenericRecord deserializedRecord = reader.read(null, decoder);

    // Verify the deserialized record
    assertEquals(deserializedRecord.get("fieldA").toString(), "valueA");
    assertEquals((int) deserializedRecord.get("fieldB"), 123);
    assertTrue((boolean) deserializedRecord.get("fieldC"));
    assertFalse(deserializedRecord.hasField("fieldD"));
  }
}
