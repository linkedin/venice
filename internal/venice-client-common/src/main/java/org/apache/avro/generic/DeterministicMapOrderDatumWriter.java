package org.apache.avro.generic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;


/**
 * This interface provides method to write map entries in a deterministic order. By default, the "deterministic order" is
 * the same as natural ordering of map keys.
 */
public interface DeterministicMapOrderDatumWriter {
  Comparator<Map.Entry<Object, Object>> COMPARATOR = (e1, e2) -> {
    Object o1 = e1.getKey();
    Object o2 = e2.getKey();

    if (Objects.requireNonNull(o1) == Objects.requireNonNull(o2)) {
      return 0;
    }

    if (o1.getClass() == o2.getClass() && o1 instanceof Comparable) {
      return ((Comparable<Object>) o1).compareTo(o2);
    }

    CharSequence cs1;
    if (o1 instanceof CharSequence) {
      cs1 = (CharSequence) o1;
    } else {
      cs1 = o1.toString();
    }

    CharSequence cs2;
    if (o2 instanceof CharSequence) {
      cs2 = (CharSequence) o2;
    } else {
      cs2 = o2.toString();
    }

    for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
      char a = cs1.charAt(i);
      char b = cs2.charAt(i);
      if (a != b) {
        return a - b;
      }
    }

    return cs1.length() - cs2.length();
  };

  void internalWrite(Schema schema, Object datum, Encoder out) throws IOException;

  default void writeMapWithDeterministicOrder(Schema schema, Object datum, Encoder out) throws IOException {
    Schema valueSchemaType = schema.getValueType();

    @SuppressWarnings("unchecked")
    Map<Object, Object> map = (Map<Object, Object>) datum;

    final int expectedMapSize = map.size();
    int actualSize = 0;
    out.writeMapStart();
    out.setItemCount(expectedMapSize);

    @SuppressWarnings("unchecked")
    Map.Entry<Object, Object>[] contentArray = map.entrySet().toArray(new Map.Entry[expectedMapSize]);
    Arrays.sort(contentArray, COMPARATOR);

    for (Map.Entry<Object, Object> entry: contentArray) {
      out.startItem();
      out.writeString(entry.getKey().toString());
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
