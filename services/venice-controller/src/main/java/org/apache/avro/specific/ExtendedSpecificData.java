package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;


public class ExtendedSpecificData extends SpecificData {
  /**
   * Get the value of a field by name. Override {@link org.apache.avro.generic.GenericData#getField(Object, String, int)}
   * to retrieve field values by name.
   * improving serialization and deserialization accuracy by using field names instead of index positions in schema.
   */
  @Override
  public Object getField(Object record, String name, int position) {
    if (record instanceof IndexedRecord) {
      return getFieldByName((IndexedRecord) record, name);
    } else {
      return super.getField(record, name, position);
    }
  }

  /**
   * Get the value of a field by name.
   */
  protected Object getFieldByName(IndexedRecord record, String fieldName) {
    Schema schema = record.getSchema();
    Schema.Field field = schema.getField(fieldName);
    if (field != null) {
      return record.get(field.pos());
    } else {
      throw new IllegalArgumentException("Field " + fieldName + " not found in schema.");
    }
  }
}
