package org.apache.avro.generic;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    Map<? extends CharSequence, Object> map = (Map<? extends CharSequence, Object>) datum;
    final int expectedMapSize = map.size();
    int actualSize = 0;
    out.writeMapStart();
    out.setItemCount(expectedMapSize);

    List<Map.Entry<? extends CharSequence, Object>> sortedEntryList = map.entrySet().stream().sorted((e1, e2) -> {
      // TODO: Replace with CharSequence#compare once the code has completely migrated to JDK11+
      CharSequence cs1 = e1.getKey();
      CharSequence cs2 = e2.getKey();
      if (Objects.requireNonNull(cs1) == Objects.requireNonNull(cs2)) {
        return 0;
      }

      if (cs1.getClass() == cs2.getClass() && cs1 instanceof Comparable) {
        return ((Comparable<Object>) cs1).compareTo(cs2);
      }

      for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
        char a = cs1.charAt(i);
        char b = cs2.charAt(i);
        if (a != b) {
          return a - b;
        }
      }

      return cs1.length() - cs2.length();
    }).collect(Collectors.toList());

    for (Map.Entry<? extends CharSequence, Object> entry: sortedEntryList) {
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
