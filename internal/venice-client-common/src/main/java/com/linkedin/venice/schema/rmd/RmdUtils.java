package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.antlr.v4.runtime.misc.NotNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Ths class is responsible for deserializing RMDs and extract some information from it.
 *
 * It borrows some methods from {@link com.linkedin.davinci.replication.merge.RmdSerDe}.
 */
public class RmdUtils {
  public static ByteBuffer serializeRmdRecord(final Schema valueSchema, GenericRecord rmdRecord) {
    byte[] rmdBytes = getRmdSerializer(valueSchema).serialize(rmdRecord, AvroSerializer.REUSE.get());
    return ByteBuffer.wrap(rmdBytes);
  }

  private static RecordSerializer<GenericRecord> getRmdSerializer(final Schema valueSchema) {
    return MapOrderingPreservingSerDeFactory.getSerializer(valueSchema);
  }

  public static GenericRecord deserializeRmdBytes(
      final Schema writerSchema,
      final Schema readerSchema,
      final ByteBuffer rmdBytes) {
    return getRmdDeserializer(writerSchema, readerSchema).deserialize(rmdBytes);
  }

  private static RecordDeserializer<GenericRecord> getRmdDeserializer(
      final Schema writerSchema,
      final Schema readerSchema) {
    return MapOrderingPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
  }

  /**
   * Returns the type of union record given tsObject is. Right now it will be either root level long or
   * generic record of per field timestamp.
   * @param tsObject
   */
  public static RmdTimestampType getRmdTimestampType(Object tsObject) {
    if (tsObject instanceof Long) {
      return RmdTimestampType.VALUE_LEVEL_TIMESTAMP;
    } else if (tsObject instanceof GenericRecord) {
      return RmdTimestampType.PER_FIELD_TIMESTAMP;
    } else {
      throw new IllegalStateException("Unexpected type of timestamp object. Got timestamp object: " + tsObject);
    }
  }

  public static long extractOffsetVectorSumFromRmd(GenericRecord replicationMetadataRecord) {
    Object offsetVectorObject = replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
    return sumOffsetVector(offsetVectorObject);
  }

  public static List<Long> extractOffsetVectorFromRmd(GenericRecord replicationMetadataRecord) {
    Object offsetVector = (List<Long>) replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
    if (offsetVector == null) {
      return Collections.emptyList();
    }
    return (List<Long>) offsetVector;
  }

  public static List<Long> extractTimestampFromRmd(GenericRecord replicationMetadataRecord) {
    // TODO: This function needs a heuristic to work on field level timestamps. At time of writing, this function
    // is only for recording the previous value of a record's timestamp, so we could consider specifying the incoming
    // operation to identify if we care about the record level timestamp, or, certain fields and then returning an
    // ordered
    // list of those timestamps to compare post resolution. I hesitate to commit to an implementation here prior to
    // putting
    // the full write compute resolution into uncommented fleshed out glory. So we'll effectively ignore operations
    // that aren't root level until then.
    if (replicationMetadataRecord == null) {
      return Collections.singletonList(0L);
    }
    Object timestampObject = replicationMetadataRecord.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = getRmdTimestampType(timestampObject);

    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return Collections.singletonList((Long) timestampObject);
    } else {
      // not supported yet so ignore it
      // TODO Must clone the results when PER_FIELD_TIMESTAMP mode is enabled to return the list.
      return Collections.singletonList(0L);
    }
  }

  /**
   * Returns a summation of all component parts to an offsetVector for vector comparison
   * @param offsetVector offsetVector to be summed
   * @return the sum of all offset vectors
   */
  public static long sumOffsetVector(Object offsetVector) {
    if (offsetVector == null) {
      return 0L;
    }
    return ((List<Long>) offsetVector).stream().reduce(0L, Long::sum);
  }

  /**
   * Checks to see if an offset vector has advanced completely beyond some base offset vector or not.
   *
   * @param baseOffset      The vector to compare against.
   * @param advancedOffset  The vector has should be advanced along.
   * @return                True if the advancedOffset vector has grown beyond the baseOffset
   */
  static public boolean hasOffsetAdvanced(@NotNull List<Long> baseOffset, @NotNull List<Long> advancedOffset) {
    if (baseOffset.size() > advancedOffset.size()) {
      // the baseoffset has more entries then the advanced one, meaning that it's seen entries from more colos
      // meaning that it's automatically further along then the second argument. We break early to avoid any
      // array out of bounds exception
      return false;
    }
    for (int i = 0; i < baseOffset.size(); i++) {
      if (advancedOffset.get(i) < baseOffset.get(i)) {
        return false;
      }
    }
    return true;
  }
}
