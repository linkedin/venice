package com.linkedin.venice.pubsub.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PubSubMessageSerializerTest {
  private VeniceKafkaSerializer<KafkaKey> mockKeySerializer;
  private VeniceKafkaSerializer<KafkaMessageEnvelope> mockValueSerializer;
  private PubSubMessageSerializer serializer;
  private KafkaKey testKey;
  private KafkaMessageEnvelope testValue;
  private String testTopic;

  @BeforeMethod
  public void setUp() {
    mockKeySerializer = mock(VeniceKafkaSerializer.class);
    mockValueSerializer = mock(VeniceKafkaSerializer.class);
    serializer = new PubSubMessageSerializer(mockKeySerializer, mockValueSerializer);
    testKey = mock(KafkaKey.class);
    testValue = mock(KafkaMessageEnvelope.class);
    testTopic = "test-topic";
  }

  @Test
  public void testSerializeKey() {
    byte[] expectedBytes = new byte[] { 1, 2, 3 };
    when(mockKeySerializer.serialize(testTopic, testKey)).thenReturn(expectedBytes);

    byte[] actualBytes = serializer.serializeKey(testTopic, testKey);

    assertNotNull(actualBytes);
    assertEquals(actualBytes, expectedBytes);
    verify(mockKeySerializer).serialize(testTopic, testKey);
  }

  @Test
  public void testSerializeValue() {
    byte[] expectedBytes = new byte[] { 4, 5, 6 };
    when(mockValueSerializer.serialize(testTopic, testValue)).thenReturn(expectedBytes);

    byte[] actualBytes = serializer.serializeValue(testTopic, testValue);

    assertNotNull(actualBytes);
    assertEquals(actualBytes, expectedBytes);
    verify(mockValueSerializer).serialize(testTopic, testValue);
  }
}
