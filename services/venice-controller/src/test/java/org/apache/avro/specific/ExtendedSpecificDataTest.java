package org.apache.avro.specific;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class ExtendedSpecificDataTest {
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

    // Create the ExtendedSpecificData object
    ExtendedSpecificData extendedSpecificData = new ExtendedSpecificData();

    // Get the value of the field by name, providing the wrong index
    Object fieldA = extendedSpecificData.getField(record, "fieldA", 1);
    assertEquals(fieldA, "valueA");
    Object fieldB = extendedSpecificData.getField(record, "fieldB", 0);
    assertEquals(fieldB, 123);

    // Get the value of field by name, providing the correct index
    Object fieldC = extendedSpecificData.getField(record, "fieldC", 2);
    assertEquals(fieldC, true);

    // Test getField method with non-existing field
    try {
      extendedSpecificData.getField(record, "nonExistingField", 0);
    } catch (IllegalArgumentException exception) {
      assertEquals("Field nonExistingField not found in schema.", exception.getMessage());
    } catch (Exception exception) {
      assertThrows(IllegalArgumentException.class, () -> {
        throw exception;
      });
    }
  }
}
