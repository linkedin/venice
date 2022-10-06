package com.linkedin.venice.serializer.avro;

import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.IndexedMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;


/**
 * {@code MapOrderPreservingDatumReader} converts map type serialized items into instances with
 * a consistent ordering of entries.
 */
public class MapOrderPreservingDatumReader<T> extends GenericDatumReader<T> {
  public MapOrderPreservingDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    if (old instanceof Collection) {
      ((Collection) old).clear();
      return old;
    } else
      return new LinkedList<>();
  }

  @Override
  protected Object newMap(Object old, int size) {
    if (old instanceof IndexedMap) {
      ((Map<?, ?>) old).clear();
      return old;
    } else {
      return new IndexedHashMap<Object, Object>(size);
    }
  }
}
