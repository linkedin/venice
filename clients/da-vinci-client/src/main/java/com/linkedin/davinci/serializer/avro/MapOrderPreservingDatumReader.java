package com.linkedin.davinci.serializer.avro;

import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.davinci.utils.IndexedMap;
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
  protected Object newMap(Object old, int size) {
    if (old instanceof IndexedMap) {
      ((Map<?, ?>) old).clear();
      return old;
    } else {
      return new IndexedHashMap<Object, Object>(size);
    }
  }
}
