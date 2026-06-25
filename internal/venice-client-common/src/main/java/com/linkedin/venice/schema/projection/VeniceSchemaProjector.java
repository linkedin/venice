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
      projected.put(writerField.pos(), src.get(fieldName));
    }
    return projected;
  }

  /**
   * Project a Replication Metadata (RMD) record down to the writer's RMD schema, in lockstep with the value.
   *
   * <p>Projection only runs when write compute is <em>not</em> enabled. In that mode the RMD timestamp is always a
   * whole-record (value-level) {@code long}, so projection reduces to copying the {@code long} timestamp and the
   * value-independent replication_checkpoint_vector onto a record under the target RMD schema. A per-field timestamp
   * only arises with write compute (partial updates), where the corresponding superset schema is used and no
   * projection is needed; encountering one here therefore indicates a misuse and is rejected.</p>
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

    // NOTE: This only handles the current (v1) RMD schema shape, which has exactly two top-level fields:
    // {@code timestamp} and {@code replication_checkpoint_vector}. Both are hard-coded below. If the RMD schema is
    // ever evolved (e.g. a new top-level field is added or these are restructured), this method will silently drop
    // the new field(s) and MUST be revisited to project them;
    Schema.Field rcvField = targetRmdSchema.getField(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    projected.put(rcvField.pos(), srcRmd.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME));

    Schema.Field targetTimestampField = targetRmdSchema.getField(TIMESTAMP_FIELD_NAME);
    Object srcTimestamp = srcRmd.get(TIMESTAMP_FIELD_NAME);
    if (RmdUtils.getRmdTimestampType(srcTimestamp) == RmdTimestampType.PER_FIELD_TIMESTAMP) {
      // Per-field timestamps only occur with write compute, where projection should never run (the superset schema is
      // used instead). Reject rather than attempt to project a per-field RMD.
      throw new VeniceException(
          "Per-field timestamp RMD is not supported for schema projection; projection only applies to value-level "
              + "(whole-record) timestamps. This indicates projection was attempted on a write-compute enabled store.");
    }
    // Whole-record (value-level) long timestamp: set the long branch of the union as-is.
    projected.put(targetTimestampField.pos(), srcTimestamp);
    return projected;
  }
}
