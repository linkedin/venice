package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;


/**
 *  {@code WriterSpecificData} extends {@link SpecificData} to provide enhanced functionality
 *  for handling specific data records.
 *
 *  <p>
 *    Usecase: When serialize a record created with newer schema by using the old schema, even though the two schemas
 *    are compatible, the field order may be different or the field may be missing in the old schema.
 *    In this case, the field index may not be correct. This class provides a way to retrieve the field value by name
 *    instead of using the index of the writer/reader schema.
 *  </p>
 *  <p> This class can be used to create SpecificDatumWriter. For example:
 *  <pre>
 *    SpecificDatumWriter<YourRecord> writer = new SpecificDatumWriter<>(writerSchema, new WriterSpecificData());
 *  </pre>
 *  </p>
 */
public class WriterSpecificData extends SpecificData {
  /**
   * Retrieves the value of a field by its name.
   * <p>
   *  This method overrides {@link org.apache.avro.generic.GenericData#getField(Object, String, int)},
   *  which normally retrieves a field value by its index.
   * </p>
   * When the record schema and writer schema has different fields order and/or extra fields in the middle of the schema
   * instead of appending at the end, the field index may not be correct. Then, when we serialize the record,
   * the field value may be incorrect (because the value is actually from another field), leading to errors during
   * serialization process.
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
   * @param record The record to retrieve the field value from.
   *               The record must be an instance of {@link IndexedRecord}.
   * @param fieldName The name of the field to retrieve. fieldName comes from writer schema.
   *                  if writer schema has fieldName while the record doesn't have it,
   *                     and writer schema version is smaller than record schema version,
   *                  => that fieldName is deleted in new version, we fail it here to ack issues faster.
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
