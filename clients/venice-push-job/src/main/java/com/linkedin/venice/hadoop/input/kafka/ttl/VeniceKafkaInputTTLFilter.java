package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.utils.VeniceProperties;


public class VeniceKafkaInputTTLFilter extends AbstractVeniceFilter<KafkaInputMapperValue> {
  private final TTLResolutionPolicy ttlPolicy;

  public VeniceKafkaInputTTLFilter(VeniceProperties props) {
    super(props);
    ttlPolicy = TTLResolutionPolicy.valueOf(props.getInt(VenicePushJob.REPUSH_TTL_POLICY));
  }

  @Override
  protected boolean apply(final KafkaInputMapperValue kafkaInputMapperValue) {
    switch (ttlPolicy) {
      case RT_WRITE_ONLY:
        // TODO implementation to retrieve RMD and parse timestamp information. See VENG-9956
        return false; // do not drop anything
      case BYPASS_BATCH_WRITE:
      case ACCEPT_BATCH_WRITE:
      default:
        throw new UnsupportedOperationException(ttlPolicy + " policy is not supported.");
    }
  }
}
