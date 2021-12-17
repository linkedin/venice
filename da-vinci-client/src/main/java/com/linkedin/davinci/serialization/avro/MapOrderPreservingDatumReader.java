package com.linkedin.davinci.serialization.avro;

import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.IndexedMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;


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
