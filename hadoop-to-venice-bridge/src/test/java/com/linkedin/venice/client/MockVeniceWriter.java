package com.linkedin.venice.client;

import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * This is an in-memory mock to stub out the {@link VeniceWriter}.
 */
public class MockVeniceWriter extends AbstractVeniceWriter<byte[], byte[]> {
  public static final Charset CHARSET = StandardCharsets.UTF_8;

  private Map<String, String> messages = new HashMap<>();
  private Map<String, Integer> keyValueSchemaIdMapping = new HashMap<>();

  public MockVeniceWriter(Properties properties) {
    super(properties.getProperty(KafkaPushJob.TOPIC_PROP));
  }

  @Override
  public Future<RecordMetadata> put(byte[] key, byte[] value, int valueSchemaId, Callback callback) {
    messages.put(new String(key, CHARSET), new String(value, CHARSET));
    keyValueSchemaIdMapping.put(new String(key), valueSchemaId);
    return null;
  }

  public String getValue(String key) {
    return messages.get(key);
  }

  public int getValueSchemaId(String key) {
    return keyValueSchemaIdMapping.get(key);
  }

  public void printMessages() {
    System.out.println("messages:");
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      System.out.println(entry.getKey() + " => " + entry.getValue());
    }
  }

  /**
   * No-op. There is nothing to close for this mock.
   */
  @Override
  public void close() {}
}
