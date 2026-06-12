package com.linkedin.venice.schema.projection;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdTimestampType;
import com.linkedin.venice.schema.rmd.RmdUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * Projects a value record (and its Replication Metadata) that was serialized against a superset schema down to a
 * chosen registered writer (target) schema.
 *
 * <p>The projection is a pure <em>structural drop</em>. The writer schema is guaranteed (by an upstream preflight
 * subset check) to be a strict subset of the input schema: every field the writer declares exists in the input and
 * is deeply, exactly equal; the input may only carry <em>extra top-level fields</em>. Projection therefore reduces
 * to: build a record under the writer schema, copy each writer field's value from the input by exact name, and let
 * the input's extra top-level fields fall away.</p>
 */
public class VeniceSchemaProjector {
  /**
   * Project a value record down to the writer value schema.
   *
   * @param src the source value record (serialized against the superset schema), or {@code null} for a tombstone
   * @param writerValueSchema the chosen registered writer value schema
   * @return a new record under {@code writerValueSchema}, or {@code null} if {@code src} is {@code null}
   */
  public GenericRecord projectValue(GenericRecord src, Schema writerValueSchema) {
    if (src == null) {
      // Tombstone (delete) - nothing to project.
      return null;
    }
    Schema srcSchema = src.getSchema();
    GenericRecord projected = new GenericData.Record(writerValueSchema);
    for (Schema.Field writerField: writerValueSchema.getFields()) {
      String fieldName = writerField.name();
      if (srcSchema.getField(fieldName) == null) {
        // Invariant: a strict-subset writer schema cannot declare a field absent from the input schema. The preflight
        // subset check should already have rejected this; backstop it here.
        throw new VeniceException(
            "Writer value field '" + fieldName + "' is absent from the input value schema; writer schema is not a "
                + "subset of the input schema.");
      }
      projected.put(writerField.pos(), GenericData.get().deepCopy(writerField.schema(), src.get(fieldName)));
    }
    return projected;
  }

  /**
   * Project a Replication Metadata (RMD) record down to the writer's RMD schema, in lockstep with the value.
   *
   * @param srcRmd the source RMD record (generated against the superset value schema), or {@code null} if the record
   *               carries no RMD (e.g. a batch record in a hybrid store)
   * @param targetRmdSchema the writer's RMD schema (generated against the writer value schema)
   * @return a new RMD record under {@code targetRmdSchema}, or {@code null} if {@code srcRmd} is {@code null}
   */
  public GenericRecord projectRmd(GenericRecord srcRmd, Schema targetRmdSchema) {
    if (srcRmd == null) {
      // A record may legitimately carry no RMD (e.g. a batch record coexisting with A/A records in a hybrid store).
      // Mirror projectValue's tombstone handling: nothing to project, so the caller writes the record without RMD.
      return null;
    }
    GenericRecord projected = new GenericData.Record(targetRmdSchema);

    // replication_checkpoint_vector is value-independent; copy verbatim.
    Schema.Field rcvField = targetRmdSchema.getField(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    projected.put(
        rcvField.pos(),
        GenericData.get().deepCopy(rcvField.schema(), srcRmd.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME)));

    Schema.Field targetTimestampField = targetRmdSchema.getField(TIMESTAMP_FIELD_NAME);
    Object srcTimestamp = srcRmd.get(TIMESTAMP_FIELD_NAME);
    if (RmdUtils.getRmdTimestampType(srcTimestamp) == RmdTimestampType.PER_FIELD_TIMESTAMP) {
      // Per-field timestamp record: rebuild against the target's per-field schema. The timestamp field is a
      // union[long, perFieldRecord]; the per-field record is the second union branch.
      GenericRecord srcPerField = (GenericRecord) srcTimestamp;
      Schema targetPerFieldSchema = targetTimestampField.schema().getTypes().get(1);
      GenericRecord targetPerField = new GenericData.Record(targetPerFieldSchema);
      for (Schema.Field targetField: targetPerFieldSchema.getFields()) {
        String fieldName = targetField.name();
        if (srcPerField.getSchema().getField(fieldName) == null) {
          // Invariant: a surviving value field must have a corresponding per-field RMD entry in the source.
          throw new VeniceException(
              "Per-field RMD entry '" + fieldName + "' is absent from the source RMD; source RMD does not cover the "
                  + "writer schema's fields.");
        }
        targetPerField
            .put(targetField.pos(), GenericData.get().deepCopy(targetField.schema(), srcPerField.get(fieldName)));
      }
      projected.put(targetTimestampField.pos(), targetPerField);
    } else {
      // Whole-record (value-level) long timestamp: copy as-is.
      projected.put(targetTimestampField.pos(), srcTimestamp);
    }
    return projected;
  }
}
