package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;


public class VeniceSpecificDatumReader<T> extends SpecificDatumReader<T> {
  public VeniceSpecificDatumReader(Schema protocolSchema, Schema compiledProtocol) {
    super(protocolSchema, compiledProtocol);
  }

  /** Called to create new array instances. To avoid concurrent modification error, returns {@link
   * VeniceConcurrentHashMap} instead.*/
  @Override
  @SuppressWarnings("unchecked")
  protected Object newMap(Object old, int size) {
    if (old instanceof VeniceConcurrentHashMap) {
      ((VeniceConcurrentHashMap<?, ?>) old).clear();
      return old;
    } else
      return new VeniceConcurrentHashMap<Object, Object>(size);
  }
}
