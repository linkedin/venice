package org.apache.avro.generic;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;


/**
 * This interface provides method to write map entries in a deterministic order. By default, the "deterministic order" is
 * the same as natural ordering of map keys.
 */
public interface DeterministicMapOrderDatumWriter {
  void internalWrite(Schema schema, Object datum, Encoder out) throws IOException;

  default void writeMapWithDeterministicOrder(Schema schema, Object datum, Encoder out) throws IOException {
    Schema valueSchemaType = schema.getValueType();
    Map map = (Map) datum;
    final int expectedMapSize = map.size();
    int actualSize = 0;
    out.writeMapStart();
    out.setItemCount(expectedMapSize);

    List<Map.Entry> sortedEntryList =
        (List<Map.Entry>) map.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());

    for (Map.Entry entry: sortedEntryList) {
      out.startItem();
      out.writeString((CharSequence) entry.getKey().toString());
      internalWrite(valueSchemaType, entry.getValue(), out);
      actualSize++;
    }

    out.writeMapEnd();
    if (actualSize != expectedMapSize) {
      throw new ConcurrentModificationException(
          "Size of map written was " + expectedMapSize + ", but number of entries written was " + actualSize + ". ");
    }
  }
}
