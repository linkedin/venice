package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class ApacheKafkaUtils {
  public static final RecordHeaders EMPTY_RECORD_HEADERS = new RecordHeaders();

  static {
    EMPTY_RECORD_HEADERS.setReadOnly();
  }

  public static RecordHeaders convertToKafkaSpecificHeaders(PubSubMessageHeaders headers) {
    if (headers == null || headers.isEmpty()) {
      return EMPTY_RECORD_HEADERS;
    }
    RecordHeaders recordHeaders = new RecordHeaders();
    for (PubSubMessageHeader header: headers) {
      recordHeaders.add(header.key(), header.value());
    }
    return recordHeaders;
  }
}
