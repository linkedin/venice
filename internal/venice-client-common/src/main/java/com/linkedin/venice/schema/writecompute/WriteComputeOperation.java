package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.LIST_OPS_NAME;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_OPS_NAME;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;


/**
 * This enum describe the possible write compute operations Venice supports.
 */
public enum WriteComputeOperation {
  /**
   * Mark to ignore the field. It's used for "partial put" and can be applied to any kind of schema.
   * It's also the default for all record fields in the write compute schema.
   */
  NO_OP_ON_FIELD("NoOp"),

  /**
   * Put a new value on an existing field. It's used for "partial put".
   */
  PUT_NEW_FIELD("PutNewField"),

  /**
   * Perform list operations on top of the original array. It can be only applied to Avro array.
   * Currently support:
   * 1. setUnion: add elements into the original array, as if it was a sorted set. (e.g.: duplicates will be pruned.)
   * 2. setDiff: remove elements from the original array, as if it was a sorted set.
   */
  LIST_OPS(
      LIST_OPS_NAME,
      new Function[] {
          schema -> AvroCompatibilityHelper
              .createSchemaField(SET_UNION, (Schema) schema, null, Collections.emptyList()),
          schema -> AvroCompatibilityHelper
              .createSchemaField(SET_DIFF, (Schema) schema, null, Collections.emptyList()) }
  ),

  /**
   * Perform map operations on top of the original map. It can be only applied to Avro map.
   * Currently support:
   * 1. mapUnion: add new entries into the original map. It overrides the value if a key has already existed in the map.
   * 2. mapDiff: remove entries from the original array.
   */
  MAP_OPS(
      MAP_OPS_NAME,
      new Function[] {
          schema -> AvroCompatibilityHelper.createSchemaField(MAP_UNION, (Schema) schema, null, Collections.emptyMap()),
          schema -> AvroCompatibilityHelper.createSchemaField(
              MAP_DIFF,
              Schema.createArray(Schema.create(Schema.Type.STRING)),
              null,
              Collections.emptyList()) }
  );

  // a name that meets class naming convention
  public final String name;

  final Optional<Function<Schema, Schema.Field>[]> params;

  WriteComputeOperation(String name) {
    this.name = name;
    this.params = Optional.empty();
  }

  WriteComputeOperation(String name, Function<Schema, Schema.Field>[] params) {
    this.name = name;
    this.params = Optional.of(params);
  }

  public String getName() {
    return name;
  }

  String getUpperCamelName() {
    if (name.isEmpty()) {
      return name;
    }

    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  public static WriteComputeOperation getFieldOperationType(Object writeComputeFieldValue) {

    if (writeComputeFieldValue instanceof IndexedRecord) {
      IndexedRecord writeComputeFieldRecord = (IndexedRecord) writeComputeFieldValue;
      String writeComputeFieldSchemaName = writeComputeFieldRecord.getSchema().getName();

      if (writeComputeFieldSchemaName.equals(NO_OP_ON_FIELD.name)) {
        return NO_OP_ON_FIELD;
      }

      if (writeComputeFieldSchemaName.endsWith(LIST_OPS.name)) {
        return LIST_OPS;
      }

      if (writeComputeFieldSchemaName.endsWith(MAP_OPS.name)) {
        return MAP_OPS;
      }
    }
    return PUT_NEW_FIELD;
  }

  public static boolean isPartialUpdateOp(GenericRecord writeComputeRecord) {
    return writeComputeRecord.getSchema().getName().endsWith(WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX);
  }
}
