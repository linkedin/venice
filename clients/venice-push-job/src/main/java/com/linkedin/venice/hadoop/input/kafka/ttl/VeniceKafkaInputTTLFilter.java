package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This class is responsible to filter records based on the RMD information and the ttl config by taking KIF input {@link KafkaInputMapperValue}.
 */
public class VeniceKafkaInputTTLFilter extends VeniceRmdTTLFilter<KafkaInputMapperValue> {
  public VeniceKafkaInputTTLFilter(final VeniceProperties props) throws IOException {
    super(props);
  }

  @Override
  protected int getSchemaId(final KafkaInputMapperValue kafkaInputMapperValue) {
    return kafkaInputMapperValue.schemaId;
  }

  protected int getRmdId(final KafkaInputMapperValue kafkaInputMapperValue) {
    return kafkaInputMapperValue.replicationMetadataVersionId;
  }

  @Override
  protected ByteBuffer getRmdPayload(final KafkaInputMapperValue kafkaInputMapperValue) {
    return kafkaInputMapperValue.replicationMetadataPayload;
  }

  /**
   * When schemeId is negative, it indicates a chunked record.
   * Skip it and pass it to Reducer as chunk will only be re-assembled at Reducer.
   * @param input
   * @return
   */
  @Override
  protected boolean skipRmdRecord(final KafkaInputMapperValue input) {
    return input.schemaId < 0;
  }
}
