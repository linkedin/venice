package com.linkedin.venice.schema.projection;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdTimestampType;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * Projects a value record (and its Replication Metadata) that was serialized against a superset schema down to a
 * chosen registered writer (target) schema.
 *
 * <p>The projection follows the writer schema, which is guaranteed (by an upstream preflight projection-subset check)
 * to be obtainable from the input schema. At every nesting level (records, array elements, map values) the input may
 * wrap a field as a nullable union ({@code [null, X]} vs writer {@code X}) and may carry extra fields the writer does
 * not declare. Projection rebuilds each record under the writer schema, unwrapping nullable inputs and dropping the
 * input's extra fields at every level. Subtrees that need no projection (the input subschema already equals the writer
 * subschema) are copied by reference rather than rebuilt.</p>
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
    return projectRecord(src, writerValueSchema);
  }

  /**
   * Rebuild {@code src} under {@code writerRecordSchema}: copy each writer field by name (projecting its value to the
   * writer's field type), and drop any extra fields the input carries.
   */
  private static GenericRecord projectRecord(GenericRecord src, Schema writerRecordSchema) {
    Schema srcSchema = src.getSchema();
    GenericRecord projected = new GenericData.Record(writerRecordSchema);
    for (Schema.Field writerField: writerRecordSchema.getFields()) {
      Schema.Field srcField = srcSchema.getField(writerField.name());
      if (srcField == null) {
        // Invariant: a projection-subset writer schema cannot declare a field absent from the input schema. The
        // preflight subset check should already have rejected this; backstop it here.
        throw new VeniceException(
            "Writer value field '" + writerField.name() + "' is absent from the input value schema; writer schema is "
                + "not a subset of the input schema.");
      }
      projected
          .put(writerField.pos(), projectField(writerField.schema(), srcField.schema(), src.get(writerField.name())));
    }
    return projected;
  }

  /**
   * Project a single field {@code value} from its input type down to the writer type, recursing through records, array
   * elements, and map values.
   */
  @SuppressWarnings("unchecked")
  private static Object projectField(Schema writerType, Schema inputType, Object value) {
    // Unwrap nullable wrapping on the input side: input null-first [null, X] against a non-union writer projects the
    // value against X. The runtime value is already the (possibly null) non-null-branch value.
    if (writerType.getType() != Schema.Type.UNION && AvroSchemaUtils.isNullableUnionPair(inputType)
        && inputType.getTypes().get(0).getType() == Schema.Type.NULL) {
      return projectField(writerType, inputType.getTypes().get(1), value);
    }
    // No projection needed when the writer and input subschemas already match: copy by reference rather than rebuild.
    // This keeps primitives and unchanged subtrees shared. OH should not emit null for a non-nullable writer field; if
    // it does, the null is copied through here and serialization (not projection) rejects it.
    if (value == null || writerType.equals(inputType)) {
      return value;
    }
    switch (writerType.getType()) {
      case RECORD:
        return projectRecord((GenericRecord) value, writerType);
      case ARRAY: {
        Schema writerElement = writerType.getElementType();
        Schema inputElement = inputType.getElementType();
        List<Object> srcList = (List<Object>) value;
        List<Object> projectedList = new ArrayList<>(srcList.size());
        for (Object element: srcList) {
          projectedList.add(projectField(writerElement, inputElement, element));
        }
        return projectedList;
      }
      case MAP: {
        Schema writerValue = writerType.getValueType();
        Schema inputValue = inputType.getValueType();
        Map<CharSequence, Object> srcMap = (Map<CharSequence, Object>) value;
        Map<CharSequence, Object> projectedMap = new HashMap<>(srcMap.size());
        for (Map.Entry<CharSequence, Object> entry: srcMap.entrySet()) {
          projectedMap.put(entry.getKey(), projectField(writerValue, inputValue, entry.getValue()));
        }
        return projectedMap;
      }
      default:
        // Primitives, enums, fixed, and unions: validation guarantees compatibility, so copy the value as-is.
        return value;
    }
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
