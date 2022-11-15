package com.linkedin.venice.client.store;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * A simple wrapper to throw exception when retrieving failed computation.
 */
public class ComputeGenericRecord implements GenericRecord {
  private final GenericRecord innerRecord;
  private final Optional<Map<String, String>> computationErrorMap;
  private final Schema valueSchema;

  /** Schema of the original record that was used to compute the result */

  public ComputeGenericRecord(GenericRecord record, Schema valueSchema) {
    this.innerRecord = record;
    this.valueSchema = valueSchema;
    Object errorMap = record.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME);
    if (errorMap != null && errorMap instanceof Map && !((Map) errorMap).isEmpty()) {
      /**
       * The reason for the following conversion is that the de-serialized Map
       * is actually a Map<Utf8, Utf8>, so {@link #get(String)} could not use it directly.
       * To avoid the String -> {@link Utf8} in {@link #get(String)}, the constructor
       * will just convert the map to be a Map<String, String>.
       */
      Map<String, String> stringErrorMap = new HashMap<>();
      ((Map) errorMap).forEach((k, v) -> stringErrorMap.put(k.toString(), v.toString()));
      computationErrorMap = Optional.of(stringErrorMap);
    } else {
      computationErrorMap = Optional.empty();
    }
  }

  @Override
  public void put(String key, Object v) {
    innerRecord.put(key, v);
  }

  @Override
  public Object get(String key) {
    if (computationErrorMap.isPresent() && computationErrorMap.get().containsKey(key)) {
      throw new VeniceException(
          "Something bad happened in Venice backend when computing this field: " + key + ", error message: "
              + computationErrorMap.get().get(key));
    }
    return innerRecord.get(key);
  }

  @Override
  public void put(int i, Object v) {
    innerRecord.put(i, v);
  }

  @Override
  public Object get(int i) {
    return innerRecord.get(i);
  }

  @Override
  public Schema getSchema() {
    return innerRecord.getSchema();
  }

  public Schema getValueSchema() {
    return valueSchema;
  }
}
