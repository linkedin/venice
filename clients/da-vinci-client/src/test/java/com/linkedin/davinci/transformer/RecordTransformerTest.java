package com.linkedin.davinci.transformer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  @Test
  public void testTransformedRecord() {
    Schema schema = createSampleSchema();

    String key = "sampleKey";
    String value = "sampleValue";

    TransformedRecord<String, String> transformedRecord = new TransformedRecord<>();
    transformedRecord.setKey(key);
    transformedRecord.setValue(value);

    assertEquals(transformedRecord.getKey(), key);
    assertEquals(transformedRecord.getValue(), value);

    String newKey = "newKey";
    transformedRecord.setKey(newKey);
    assertEquals(transformedRecord.getKey(), newKey);

    String newValue = "newValue";
    transformedRecord.setValue(newValue);
    assertEquals(transformedRecord.getValue(), newValue);

    byte[] keyBytes = transformedRecord.getKeyBytes(schema);
    assertNotNull(keyBytes);

    ByteBuffer valueBytes = transformedRecord.getValueBytes(schema);
    assertNotNull(valueBytes);
    assertEquals(valueBytes.getInt(), 1);
  }

  private Schema createSampleSchema() {
    String schemaString = "{\n" + "  \"type\": \"string\"\n" + "}\n";
    return new Schema.Parser().parse(schemaString);
  }

  public class TestRecordTransformer
      implements DaVinciRecordTransformer<Integer, String, TransformedRecord<Integer, String>> {
    public Schema getKeyOutputSchema() {
      return Schema.create(Schema.Type.INT);
    }

    public Schema getValueOutputSchema() {
      return Schema.create(Schema.Type.STRING);
    }

    public TransformedRecord<Integer, String> put(Lazy<Integer> key, Lazy<String> value) {
      TransformedRecord<Integer, String> transformedRecord = new TransformedRecord<>();
      transformedRecord.setKey(key.get());
      transformedRecord.setValue(value.get() + "Transformed");
      return transformedRecord;
    }
  }

  @Test
  public void testRecordTransformer() {
    DaVinciRecordTransformer<Integer, String, TransformedRecord<Integer, String>> recordTransformer =
        new TestRecordTransformer();

    Schema keyOutputSchema = recordTransformer.getKeyOutputSchema();
    assertEquals(keyOutputSchema.getType(), Schema.Type.INT);

    Schema valueOutputSchema = recordTransformer.getValueOutputSchema();
    assertEquals(valueOutputSchema.getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    TransformedRecord<Integer, String> transformedRecord = recordTransformer.put(lazyKey, lazyValue);
    assertEquals(Optional.ofNullable(transformedRecord.getKey()), Optional.ofNullable(42));
    assertEquals(transformedRecord.getValue(), "SampleValueTransformed");

    Lazy<Integer> lazyDeleteKey = Lazy.of(() -> 99);
    TransformedRecord<Integer, String> deletedRecord = recordTransformer.delete(lazyDeleteKey);
    assertNull(deletedRecord);
  }

}
