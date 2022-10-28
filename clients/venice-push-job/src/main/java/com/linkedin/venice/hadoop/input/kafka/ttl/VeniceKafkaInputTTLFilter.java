package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.schema.HDFSRmdSchemaSource;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is responsible to filter records in Kafka Input Format based on the ttl config.
 */
public class VeniceKafkaInputTTLFilter extends AbstractVeniceFilter<KafkaInputMapperValue> {
  private final TTLResolutionPolicy ttlPolicy;
  private final long ttlInMs;
  private final HDFSRmdSchemaSource schemaSource;
  private final Map<RmdVersionId, Schema> rmdMapping;

  public VeniceKafkaInputTTLFilter(VeniceProperties props) throws IOException {
    super(props);
    ttlPolicy = TTLResolutionPolicy.valueOf(props.getInt(VenicePushJob.REPUSH_TTL_POLICY));
    ttlInMs = TimeUnit.SECONDS.toMillis(props.getLong(VenicePushJob.REPUSH_TTL_IN_SECONDS));
    schemaSource = new HDFSRmdSchemaSource(props.getString(VenicePushJob.RMD_SCHEMA_DIR));
    rmdMapping = schemaSource.fetchSchemas();
  }

  @Override
  public boolean apply(final KafkaInputMapperValue kafkaInputMapperValue) {
    // if the schemaId is negative, that's a chunked record. Ignore and filter in reducer stage
    if (kafkaInputMapperValue.schemaId < 0) {
      return false;
    }
    Instant curTime = Instant.now();
    switch (ttlPolicy) {
      case RT_WRITE_ONLY:
        Instant timestamp = Instant.ofEpochMilli(getTimeStampFromRmd(kafkaInputMapperValue));
        return ChronoUnit.MILLIS.between(timestamp, curTime) > ttlInMs;
      default:
        throw new UnsupportedOperationException(ttlPolicy + " policy is not supported.");
    }
  }

  @Override
  public void close() {
    schemaSource.close();
  }

  private Long getTimeStampFromRmd(final KafkaInputMapperValue input) {
    if (input.replicationMetadataPayload == null || !input.replicationMetadataPayload.hasRemaining()) {
      throw new IllegalStateException(
          String.format("The record at offset %s doesn't contain required RMD field.", input.offset));
    }
    int id = input.replicationMetadataVersionId, valueSchemaId = input.schemaId;
    Schema schema = rmdMapping.get(new RmdVersionId(valueSchemaId, id));
    GenericRecord record = RmdUtils.deserializeRmdBytes(schema, schema, input.replicationMetadataPayload);
    return RmdUtils.extractTimestampFromRmd(record)
        .stream()
        .mapToLong(v -> v)
        .max()
        .orElseThrow(NoSuchElementException::new);
  }
}
