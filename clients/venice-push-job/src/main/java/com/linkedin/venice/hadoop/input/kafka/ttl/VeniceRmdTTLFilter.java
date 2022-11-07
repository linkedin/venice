package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.schema.HDFSRmdSchemaSource;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NoSuchElementException;
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
  private final long ttlInMs;
  private final HDFSRmdSchemaSource schemaSource;
  protected final Map<RmdVersionId, Schema> rmdMapping;

  public VeniceRmdTTLFilter(final VeniceProperties props) throws IOException {
    super(props);
    ttlPolicy = TTLResolutionPolicy.valueOf(props.getInt(VenicePushJob.REPUSH_TTL_POLICY));
    ttlInMs = TimeUnit.SECONDS.toMillis(props.getLong(VenicePushJob.REPUSH_TTL_IN_SECONDS));
    schemaSource = new HDFSRmdSchemaSource(props.getString(VenicePushJob.RMD_SCHEMA_DIR));
    rmdMapping = schemaSource.fetchSchemas();
  }

  @Override
  public boolean apply(final INPUT_VALUE value) {
    if (skipRmdRecord(value)) {
      return false;
    }
    Instant curTime = Instant.now();
    switch (ttlPolicy) {
      case RT_WRITE_ONLY:
        Instant timestamp = Instant.ofEpochMilli(getTimeStampFromRmdRecord(value));
        return ChronoUnit.MILLIS.between(timestamp, curTime) > ttlInMs;
      default:
        throw new UnsupportedOperationException(ttlPolicy + " policy is not supported.");
    }
  }

  @Override
  public void close() {
    schemaSource.close();
  }

  public long getTimeStampFromRmdRecord(final INPUT_VALUE value) {
    ByteBuffer rmdPayload = getRmdPayload(value);
    if (rmdPayload == null || !rmdPayload.hasRemaining()) {
      throw new IllegalStateException(
          "The record doesn't contain required RMD field. Please check if your store has A/A enabled");
    }
    int id = getRmdId(value), valueSchemaId = getSchemaId(value);
    Schema schema = rmdMapping.get(new RmdVersionId(valueSchemaId, id));
    GenericRecord record = RmdUtils.deserializeRmdBytes(schema, schema, rmdPayload);
    return RmdUtils.extractTimestampFromRmd(record)
        .stream()
        .mapToLong(v -> v)
        .max()
        .orElseThrow(NoSuchElementException::new);
  }

  protected abstract int getSchemaId(final INPUT_VALUE value);

  protected abstract int getRmdId(final INPUT_VALUE value);

  protected abstract ByteBuffer getRmdPayload(final INPUT_VALUE value);

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
