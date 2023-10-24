package com.linkedin.venice.hadoop.input.kafka.ttl;

import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;

import com.linkedin.davinci.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.schema.merge.UpdateResultStatus;
import com.linkedin.davinci.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.schema.HDFSSchemaSource;
import com.linkedin.venice.schema.rmd.RmdTimestampType;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is responsible to filter records based on the RMD information and the ttl config.
 * It requires RMD schemas for a given store from an existing HDFS directory to be able to parse timestamp information in RMD.
 * @param <INPUT_VALUE>, the value contains schemaID, rmdId and rmdPayload that are required to retrieve RMD timestamp.
 */
public abstract class VeniceRmdTTLFilter<INPUT_VALUE> extends AbstractVeniceFilter<INPUT_VALUE> {
  private final TTLResolutionPolicy ttlPolicy;
  private final long filterTimestamp;
  private final HDFSSchemaSource schemaSource;
  protected final Map<RmdVersionId, Schema> rmdSchemaMap;
  protected final Map<Integer, Schema> valueSchemaMap;
  private final Map<RmdVersionId, RecordDeserializer<GenericRecord>> rmdDeserializerCache;
  private final Map<Integer, RecordDeserializer<GenericRecord>> valueDeserializerCache;
  private final Map<RmdVersionId, RecordSerializer<GenericRecord>> rmdSerializerCache;
  private final Map<Integer, RecordSerializer<GenericRecord>> valueSerializerCache;
  private final MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();

  public VeniceRmdTTLFilter(final VeniceProperties props) throws IOException {
    super();
    ttlPolicy = TTLResolutionPolicy.valueOf(props.getInt(VenicePushJob.REPUSH_TTL_POLICY));
    long ttlInMs = TimeUnit.SECONDS.toMillis(props.getLong(VenicePushJob.REPUSH_TTL_IN_SECONDS));
    long ttlStartTimestamp = props.getLong(VenicePushJob.REPUSH_TTL_START_TIMESTAMP);
    this.filterTimestamp = ttlStartTimestamp - ttlInMs - 1;
    this.schemaSource = new HDFSSchemaSource(
        props.getString(VenicePushJob.VALUE_SCHEMA_DIR),
        props.getString(VenicePushJob.RMD_SCHEMA_DIR));
    this.rmdSchemaMap = schemaSource.fetchRmdSchemas();
    this.valueSchemaMap = schemaSource.fetchValueSchemas();
    this.rmdDeserializerCache = new VeniceConcurrentHashMap<>();
    this.valueDeserializerCache = new VeniceConcurrentHashMap<>();
    this.rmdSerializerCache = new VeniceConcurrentHashMap<>();
    this.valueSerializerCache = new VeniceConcurrentHashMap<>();
  }

  @Override
  public boolean checkAndMaybeFilterValue(final INPUT_VALUE value) {
    if (skipRmdRecord(value)) {
      return false;
    }
    if (Objects.requireNonNull(ttlPolicy) == TTLResolutionPolicy.RT_WRITE_ONLY) {
      return validateByTTLandMaybeUpdateValue(value);
    }
    throw new UnsupportedOperationException(ttlPolicy + " policy is not supported.");
  }

  @Override
  public void close() {
    schemaSource.close();
  }

  boolean validateByTTLandMaybeUpdateValue(final INPUT_VALUE value) {
    ByteBuffer rmdPayload = getRmdPayload(value);
    if (rmdPayload == null || !rmdPayload.hasRemaining()) {
      throw new IllegalStateException(
          "The record doesn't contain required RMD field. Please check if your store has A/A enabled");
    }
    int valueSchemaId = getSchemaId(value);
    int id = getRmdProtocolId(value);
    RmdVersionId rmdVersionId = new RmdVersionId(valueSchemaId, id);
    GenericRecord rmdRecord =
        rmdDeserializerCache.computeIfAbsent(rmdVersionId, this::generateRmdDeserializer).deserialize(rmdPayload);
    Object rmdTimestampObject = rmdRecord.get(TIMESTAMP_FIELD_POS);
    RmdTimestampType rmdTimestampType = RmdUtils.getRmdTimestampType(rmdTimestampObject);
    // For value-level RMD timestamp, just compare the value with the filter TS.
    if (rmdTimestampType.equals(RmdTimestampType.VALUE_LEVEL_TIMESTAMP)) {
      return (long) rmdTimestampObject <= filterTimestamp;
    }
    RecordDeserializer<GenericRecord> valueDeserializer =
        valueDeserializerCache.computeIfAbsent(valueSchemaId, this::generateValueDeserializer);
    GenericRecord valueRecord = valueDeserializer.deserialize(getValuePayload(value));
    UpdateResultStatus updateResultStatus =
        mergeRecordHelper.deleteRecord(valueRecord, (GenericRecord) rmdTimestampObject, filterTimestamp, 0);
    if (updateResultStatus.equals(UpdateResultStatus.COMPLETELY_UPDATED)) {
      // This means the record is fully stale, we should drop it.
      return true;
    }
    if (updateResultStatus.equals(UpdateResultStatus.NOT_UPDATED_AT_ALL)) {
      // This means the whole record is newer than TTL filter threshold timestamp, and we should keep it.
      return false;
    }
    // Part of the data has been wiped out by DELETE operation, and we should update the input's value and RMD payload.
    RecordSerializer<GenericRecord> valueSerializer =
        valueSerializerCache.computeIfAbsent(valueSchemaId, this::generateValueSerializer);
    RecordSerializer<GenericRecord> rmdSerializer =
        rmdSerializerCache.computeIfAbsent(rmdVersionId, this::generateRmdSerializer);
    updateValuePayload(value, valueSerializer.serialize(valueRecord));
    updateRmdPayload(value, ByteBuffer.wrap(rmdSerializer.serialize(rmdRecord)));
    return false;
  }

  RecordDeserializer<GenericRecord> generateRmdDeserializer(RmdVersionId rmdVersionId) {
    Schema schema = rmdSchemaMap.get(rmdVersionId);
    return MapOrderingPreservingSerDeFactory.getDeserializer(schema, schema);
  }

  RecordDeserializer<GenericRecord> generateValueDeserializer(int valueSchemaId) {
    Schema schema = valueSchemaMap.get(valueSchemaId);
    return MapOrderingPreservingSerDeFactory.getDeserializer(schema, schema);
  }

  RecordSerializer<GenericRecord> generateRmdSerializer(RmdVersionId rmdVersionId) {
    Schema schema = rmdSchemaMap.get(rmdVersionId);
    return MapOrderingPreservingSerDeFactory.getSerializer(schema);
  }

  RecordSerializer<GenericRecord> generateValueSerializer(int valueSchemaId) {
    Schema schema = valueSchemaMap.get(valueSchemaId);
    return MapOrderingPreservingSerDeFactory.getSerializer(schema);
  }

  protected abstract int getSchemaId(final INPUT_VALUE value);

  protected abstract int getRmdProtocolId(final INPUT_VALUE value);

  protected abstract ByteBuffer getRmdPayload(final INPUT_VALUE value);

  protected abstract ByteBuffer getValuePayload(final INPUT_VALUE value);

  protected abstract void updateRmdPayload(final INPUT_VALUE value, ByteBuffer payload);

  protected abstract void updateValuePayload(final INPUT_VALUE value, byte[] payload);

  /**
   * Define how records could be skipped if certain conditions are met.
   * Do not skip by default.
   * @param value
   * @return true if this record should not be filtered and skipped.
   */
  protected boolean skipRmdRecord(final INPUT_VALUE value) {
    return false;
  }
}
