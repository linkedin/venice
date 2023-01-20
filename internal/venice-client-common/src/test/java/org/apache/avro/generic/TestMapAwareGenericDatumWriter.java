package org.apache.avro.generic;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMapAwareGenericDatumWriter {
  /**
   * a helper String class that can config hash code manually
   */
  private static class TestString extends Utf8 {
    private int i;
    private String s;

    TestString(int i, String s) {
      super(s);
      this.s = s;
      this.i = i;
    }

    @Override
    public int hashCode() {
      return i;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestString object = (TestString) o;
      return object.i == this.i && object.s.equals(this.s);
    }
  }

  @Test
  public void testWriterCanWriteMapInOrder() throws IOException {
    // construct 2 elements that have the same hash code
    TestString string1 = new TestString(1, "a");
    TestString string2 = new TestString(1, "b");

    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));

    /*construct 2 maps that have 2 entries with the same key hash code.
    This guarantee that the collision will happen when inserting the
    entries. That being said, if we insert the entries in different
    order, it will be persisted in different order in the map. (By
    default, Java HashMap uses linked list to persist collided entries
    if the number of collided entries is less than 8)*/
    Map<TestString, Integer> map1 = new HashMap<>();
    map1.put(string1, 1);
    map1.put(string2, 2);
    Map<TestString, Integer> map2 = new HashMap<>();
    map2.put(string2, 2);
    map2.put(string1, 1);

    // Since GenericDatumWriter doesn't sort the entries before serializing,
    // the output byte arrays will look differently between map1 and map2.
    GenericDatumWriter genericDatumWriter = new GenericDatumWriter(mapSchema);
    Assert.assertFalse(Arrays.equals(serialize(genericDatumWriter, map1), serialize(genericDatumWriter, map2)));

    DeterministicMapOrderGenericDatumWriter mapAwareGenericDatumWriter =
        new DeterministicMapOrderGenericDatumWriter(mapSchema);
    Assert.assertTrue(
        Arrays.equals(serialize(mapAwareGenericDatumWriter, map1), serialize(mapAwareGenericDatumWriter, map2)));
  }

  // helper method that serializes the object
  private byte[] serialize(GenericDatumWriter writer, Map map) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(output, true, null);

    writer.write(map, encoder);
    encoder.flush();

    return output.toByteArray();
  }
}
