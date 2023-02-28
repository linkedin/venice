package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class ApacheKafkaUtils {
  public static final RecordHeaders EMPTY_RECORD_HEADERS = new RecordHeaders();

  public static RecordHeaders convertToKafkaSpecificHeaders(PubSubMessageHeaders headers) {
    if (headers == null) {
      return EMPTY_RECORD_HEADERS;
    }
    RecordHeaders recordHeaders = new RecordHeaders();
    headers.toList().forEach(header -> recordHeaders.add(header.key(), header.value()));
    return recordHeaders;
  }
}
