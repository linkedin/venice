package com.linkedin.venice.pubsub.adapter.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.Test;


public class ApacheKafkaUtilsTest {
  @Test
  public void testConvertToKafkaSpecificHeadersWhenPubsubMessageHeadersIsNullOrEmpty() {
    Supplier<PubSubMessageHeaders>[] suppliers =
        new Supplier[] { () -> null, () -> new PubSubMessageHeaders(), () -> EmptyPubSubMessageHeaders.SINGLETON };
    for (Supplier<PubSubMessageHeaders> supplier: suppliers) {
      RecordHeaders recordHeaders = ApacheKafkaUtils.convertToKafkaSpecificHeaders(supplier.get());
      assertNotNull(recordHeaders);
      assertEquals(recordHeaders.toArray().length, 0);
      assertThrows(() -> recordHeaders.add("foo", "bar".getBytes()));

      RecordHeaders recordHeaders2 = ApacheKafkaUtils.convertToKafkaSpecificHeaders(supplier.get());
      assertNotNull(recordHeaders2);
      assertEquals(recordHeaders2.toArray().length, 0);
      assertThrows(() -> recordHeaders2.add("foo", "bar".getBytes()));

      assertSame(recordHeaders, recordHeaders2);
    }
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
