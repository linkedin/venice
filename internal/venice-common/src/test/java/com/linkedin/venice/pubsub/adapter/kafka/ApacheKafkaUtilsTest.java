package com.linkedin.venice.pubsub.adapter.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.Test;


public class ApacheKafkaUtilsTest {
  @Test
  public void testConvertToKafkaSpecificHeadersWhenPubsubMessageHeadersIsNull() {
    RecordHeaders recordHeaders = ApacheKafkaUtils.convertToKafkaSpecificHeaders(null);
    assertNotNull(recordHeaders);
    assertEquals(recordHeaders.toArray().length, 0);
  }

  @Test
  public void testConvertToKafkaSpecificHeaders() {
    Map<String, String> headerMap = new LinkedHashMap<>();
    headerMap.put("key-0", "val-0");
    headerMap.put("key-1", "val-1");
    headerMap.put("key-2", "val-2");
    PubSubMessageHeaders pubsubMessageHeaders = new PubSubMessageHeaders();
    for (Map.Entry<String, String> entry: headerMap.entrySet()) {
      pubsubMessageHeaders.add(new PubSubMessageHeader(entry.getKey(), entry.getValue().getBytes()));
    }

    RecordHeaders recordHeaders = ApacheKafkaUtils.convertToKafkaSpecificHeaders(pubsubMessageHeaders);
    assertEquals(recordHeaders.toArray().length, pubsubMessageHeaders.toList().size());
    for (Header header: recordHeaders.toArray()) {
      assertTrue(headerMap.containsKey(header.key()));
      assertEquals(header.value(), headerMap.get(header.key()).getBytes());
    }
  }
}
