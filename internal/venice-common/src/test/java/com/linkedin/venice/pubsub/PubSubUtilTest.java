package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL_LEGACY;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL_LEGACY;
import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PubSubUtil}.
 */
public class PubSubUtilTest {
  @Test
  public void testGetPubSubBrokerAddressFromProperties() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9090");
    assertEquals(PubSubUtil.getPubSubBrokerAddress(props), "localhost:9090");

    props.clear();
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "localhost:9090");
    assertEquals(PubSubUtil.getPubSubBrokerAddress(props), "localhost:9090");
  }

  @Test
  public void testGetPubSubBrokerAddressFromVeniceProperties() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9090");
    VeniceProperties veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubBrokerAddress(veniceProps), "localhost:9090");

    props.clear();
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "localhost:9090");
    veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubBrokerAddress(veniceProps), "localhost:9090");
  }

  @Test
  public void testGetPubSubBrokerAddressWithDefaultWithDefault() {
    Properties props = new Properties();
    VeniceProperties veniceProps = new VeniceProperties(props);
    String result = PubSubUtil.getPubSubBrokerAddressWithDefault(veniceProps, "default://broker");
    assertEquals(result, "default://broker");
  }

  @Test
  public void testAddPubSubBrokerAddress() {
    Properties props = new Properties();
    PubSubUtil.addPubSubBrokerAddress(props, "broker-address");
    assertEquals(props.getProperty(PUBSUB_BROKER_ADDRESS), "broker-address");
    assertEquals(props.getProperty(KAFKA_BOOTSTRAP_SERVERS), "broker-address");
  }

  @Test
  public void testGeneratePubSubClientId() {
    String clientId = PubSubUtil.generatePubSubClientId(PubSubClientType.CONSUMER, "my-client", "my-broker");
    assertTrue(clientId.startsWith("CONSUMER-my-client-"));
    assertTrue(clientId.contains("my-broker"));
  }

  @Test
  public void testGeneratePubSubClientIdWithNulls() {
    String clientId = PubSubUtil.generatePubSubClientId(PubSubClientType.ADMIN, null, null);
    assertTrue(clientId.startsWith("ADMIN--"));
  }

  @Test
  public void testClientConfigPrefixes() {
    assertEquals(PubSubUtil.getPubSubProducerConfigPrefix("a."), "pubsub.a.producer.");
    assertEquals(PubSubUtil.getPubSubConsumerConfigPrefix("a."), "pubsub.a.consumer.");
    assertEquals(PubSubUtil.getPubSubAdminConfigPrefix("a."), "pubsub.a.admin.");
  }

  @Test
  public void testClientConfigPrefixValidation() {
    assertThrows(IllegalArgumentException.class, () -> PubSubUtil.getPubSubProducerConfigPrefix("bad"));
    assertThrows(IllegalArgumentException.class, () -> PubSubUtil.getPubSubConsumerConfigPrefix(null));
  }

  @Test
  public void testGetPubSubBrokerAddressOrFailWithProperties() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "broker-from-pubsub");
    assertEquals(PubSubUtil.getPubSubBrokerAddressOrFail(props), "broker-from-pubsub");

    props.clear();
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "broker-from-kafka");
    assertEquals(PubSubUtil.getPubSubBrokerAddressOrFail(props), "broker-from-kafka");

    props.clear();
    assertThrows(IllegalArgumentException.class, () -> PubSubUtil.getPubSubBrokerAddressOrFail(props));
  }

  @Test
  public void testGetPubSubBrokerAddressOrFailWithVeniceProperties() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "broker-from-pubsub");
    VeniceProperties veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubBrokerAddressOrFail(veniceProps), "broker-from-pubsub");

    props.clear();
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "broker-from-kafka");
    veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubBrokerAddressOrFail(veniceProps), "broker-from-kafka");

    props.clear();
    veniceProps = new VeniceProperties(props);
    VeniceProperties finalVeniceProps = veniceProps;
    assertThrows(UndefinedPropertyException.class, () -> PubSubUtil.getPubSubBrokerAddressOrFail(finalVeniceProps));
  }

  @Test
  public void testGetPubSubSecurityProtocolOrDefault() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_SECURITY_PROTOCOL, "SSL");
    VeniceProperties veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.SSL);

    props.clear();
    props.setProperty(KAFKA_SECURITY_PROTOCOL_LEGACY, "SASL_SSL");
    veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.SASL_SSL);

    props.clear();
    veniceProps = new VeniceProperties(props);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.PLAINTEXT);
  }

  @Test
  public void testIsPubSubSslProtocol() {
    assertTrue(PubSubUtil.isPubSubSslProtocol("SSL"));
    assertFalse(PubSubUtil.isPubSubSslProtocol("PLAINTEXT"));
    assertTrue(PubSubUtil.isPubSubSslProtocol("SASL_SSL"));
    assertFalse(PubSubUtil.isPubSubSslProtocol("SASL_PLAINTEXT"));

    assertTrue(PubSubUtil.isPubSubSslProtocol(PubSubSecurityProtocol.SSL));
    assertFalse(PubSubUtil.isPubSubSslProtocol(PubSubSecurityProtocol.PLAINTEXT));
    assertTrue(PubSubUtil.isPubSubSslProtocol(PubSubSecurityProtocol.SASL_SSL));
    assertFalse(PubSubUtil.isPubSubSslProtocol(PubSubSecurityProtocol.SASL_PLAINTEXT));
  }

  @Test
  public void testResolveProtocolFromKafkaLegacyKey() {
    Properties props = new Properties();
    props.setProperty(KAFKA_SECURITY_PROTOCOL_LEGACY, "SSL");
    VeniceProperties veniceProps = new VeniceProperties(props);

    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.SSL);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(props), PubSubSecurityProtocol.SSL);
  }

  @Test
  public void testResolveProtocolFromPubSubLegacyKey() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_SECURITY_PROTOCOL_LEGACY, "SASL_SSL");
    VeniceProperties veniceProps = new VeniceProperties(props);

    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.SASL_SSL);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(props), PubSubSecurityProtocol.SASL_SSL);
  }

  @Test
  public void testResolveProtocolFromNewKey() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_SECURITY_PROTOCOL, "SASL_PLAINTEXT");
    VeniceProperties veniceProps = new VeniceProperties(props);

    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.SASL_PLAINTEXT);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(props), PubSubSecurityProtocol.SASL_PLAINTEXT);
  }

  @Test
  public void testResolveDefaultProtocolWhenNoKeyProvided() {
    VeniceProperties veniceProps = new VeniceProperties(new Properties());
    Properties props = new Properties();

    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProps), PubSubSecurityProtocol.PLAINTEXT);
    assertEquals(PubSubUtil.getPubSubSecurityProtocolOrDefault(props), PubSubSecurityProtocol.PLAINTEXT);
  }

  @Test
  public void testCalculateSeekOffset() {
    // Case 1: Inclusive = true, should return the same offset
    assertEquals(PubSubUtil.calculateSeekOffset(100L, true), 100L, "Inclusive seek should return the base offset");

    // Case 2: Inclusive = false, should return base offset + 1
    assertEquals(PubSubUtil.calculateSeekOffset(100L, false), 101L, "Exclusive seek should return base offset + 1");
  }

  @Test
  public void testGetBase64EncodedString() {
    // Test with normal byte array
    byte[] testBytes =
        ByteUtils.extractByteArray(new ApacheKafkaOffsetPosition(12345L).getPositionWireFormat().getRawBytes());
    String encoded = PubSubUtil.getBase64EncodedString(testBytes);
    assertEquals(encoded, "8sAB", "Should encode byte array to Base64 string");

    // Test with empty byte array
    byte[] emptyBytes = new byte[0];
    String encodedEmpty = PubSubUtil.getBase64EncodedString(emptyBytes);
    assertTrue(encodedEmpty.isEmpty(), "Should encode empty byte array to empty string");

    // Test with null input
    String encodedNull = PubSubUtil.getBase64EncodedString(null);
    assertTrue(encodedNull.isEmpty(), "Should return null for null input");

    // Test with binary data
    byte[] binaryData = { 0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE };
    String encodedBinary = PubSubUtil.getBase64EncodedString(binaryData);
    assertEquals(encodedBinary, "AAEC//4=", "Should encode binary data correctly");
  }

  @Test
  public void testGetBase64DecodedBytes() {
    // Test with normal Base64 string
    String base64String = "8sAB";
    PubSubPosition position = PubSubPositionDeserializer.DEFAULT_DESERIALIZER.toPosition(
        new PubSubPositionWireFormat(
            APACHE_KAFKA_OFFSET_POSITION_TYPE_ID,
            ByteBuffer.wrap(PubSubUtil.getBase64DecodedBytes(base64String))));
    assertEquals(position, new ApacheKafkaOffsetPosition(12345L), "Should decode Base64 string to original bytes");

    // Test with empty string
    String emptyString = "";
    byte[] decodedEmpty = PubSubUtil.getBase64DecodedBytes(emptyString);
    assertEquals(decodedEmpty.length, 0, "Should decode empty string to empty byte array");

    // Test with binary data
    String binaryBase64 = "AAEC//4=";
    byte[] decodedBinary = PubSubUtil.getBase64DecodedBytes(binaryBase64);
    byte[] expectedBinary = { 0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE };
    assertEquals(decodedBinary, expectedBinary, "Should decode binary Base64 data correctly");
  }

  @Test
  public void testGetBase64DecodedBytesWithInvalidInput() {
    // Test with invalid Base64 string
    assertThrows(IllegalArgumentException.class, () -> PubSubUtil.getBase64DecodedBytes("Invalid Base64!"));

    // Test with null input - this will throw NPE from Base64.getDecoder().decode()
    assertThrows(NullPointerException.class, () -> PubSubUtil.getBase64DecodedBytes(null));
  }

  @Test
  public void testBase64RoundTrip() {
    // Test round-trip encoding and decoding
    byte[] originalData =
        ByteUtils.extractByteArray(new ApacheKafkaOffsetPosition(12345L).getPositionWireFormat().getRawBytes());

    // Encode then decode
    String encoded = PubSubUtil.getBase64EncodedString(originalData);
    byte[] decoded = PubSubUtil.getBase64DecodedBytes(encoded);

    assertEquals(decoded, originalData, "Round-trip encoding/decoding should preserve original data");
    assertEquals(new String(decoded), new String(originalData), "Round-trip should preserve string content");

    // Test with various data sizes
    for (int size: new int[] { 1, 10, 100, 1000 }) {
      byte[] testData = new byte[size];
      for (int i = 0; i < size; i++) {
        testData[i] = (byte) (i % 256);
      }

      String encodedTest = PubSubUtil.getBase64EncodedString(testData);
      byte[] decodedTest = PubSubUtil.getBase64DecodedBytes(encodedTest);
      assertEquals(decodedTest, testData, "Round-trip should work for " + size + " bytes");
    }
  }

  @Test
  public void testComputeOffsetDeltaNullPositionsThrowsException() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);
    PubSubPosition validPosition = new ApacheKafkaOffsetPosition(100L);

    // Case 1: First position is null
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.computeOffsetDelta(partition, null, validPosition, consumerAdapter));

    // Case 2: Second position is null
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.computeOffsetDelta(partition, validPosition, null, consumerAdapter));

    // Case 3: Both positions are null
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.computeOffsetDelta(partition, null, null, consumerAdapter));
  }

  @Test
  public void testComputeOffsetDeltaBothNonSymbolicPositions() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);

    // Case 1: Both non-symbolic positions with positive delta
    PubSubPosition pos1 = new ApacheKafkaOffsetPosition(150L);
    PubSubPosition pos2 = new ApacheKafkaOffsetPosition(100L);
    long delta = PubSubUtil.computeOffsetDelta(partition, pos1, pos2, consumerAdapter);
    assertEquals(delta, 50L, "Delta should be 150 - 100 = 50");

    // Case 2: Both non-symbolic positions with negative delta
    PubSubPosition pos3 = new ApacheKafkaOffsetPosition(75L);
    PubSubPosition pos4 = new ApacheKafkaOffsetPosition(100L);
    delta = PubSubUtil.computeOffsetDelta(partition, pos3, pos4, consumerAdapter);
    assertEquals(delta, -25L, "Delta should be 75 - 100 = -25");

    // Case 3: Both non-symbolic positions with zero delta
    PubSubPosition pos5 = new ApacheKafkaOffsetPosition(100L);
    PubSubPosition pos6 = new ApacheKafkaOffsetPosition(100L);
    delta = PubSubUtil.computeOffsetDelta(partition, pos5, pos6, consumerAdapter);
    assertEquals(delta, 0L, "Delta should be 100 - 100 = 0");
  }

  @Test
  public void testComputeOffsetDeltaEqualSymbolicPositions() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);

    // Mock the symbolic position resolution
    PubSubPosition earliestResolved = new ApacheKafkaOffsetPosition(0L);
    PubSubPosition latestResolved = new ApacheKafkaOffsetPosition(1000L);
    when(consumerAdapter.beginningPosition(partition)).thenReturn(earliestResolved);
    when(consumerAdapter.endPosition(partition)).thenReturn(latestResolved);

    // Case 1: Both EARLIEST positions
    long delta = PubSubUtil.computeOffsetDelta(
        partition,
        PubSubSymbolicPosition.EARLIEST,
        PubSubSymbolicPosition.EARLIEST,
        consumerAdapter);
    assertEquals(delta, 0L, "Delta between two EARLIEST positions should be 0");

    // Case 2: Both LATEST positions
    delta = PubSubUtil
        .computeOffsetDelta(partition, PubSubSymbolicPosition.LATEST, PubSubSymbolicPosition.LATEST, consumerAdapter);
    assertEquals(delta, 0L, "Delta between two LATEST positions should be 0");
  }

  @Test
  public void testComputeOffsetDeltaEarliestAndNonSymbolic() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);

    // Mock the symbolic position resolution - return the symbolic position itself
    // This allows the symbolic logic in computeOffsetDelta to be triggered
    when(consumerAdapter.beginningPosition(partition)).thenReturn(PubSubSymbolicPosition.EARLIEST);

    // Case 1: EARLIEST first, non-symbolic second
    PubSubPosition nonSymbolic = new ApacheKafkaOffsetPosition(100L);
    long delta =
        PubSubUtil.computeOffsetDelta(partition, PubSubSymbolicPosition.EARLIEST, nonSymbolic, consumerAdapter);
    assertEquals(delta, -100L, "Delta should be -offset2 = -100");

    // Case 2: Non-symbolic first, EARLIEST second
    delta = PubSubUtil.computeOffsetDelta(partition, nonSymbolic, PubSubSymbolicPosition.EARLIEST, consumerAdapter);
    assertEquals(delta, 100L, "Delta should be offset1 = 100");

    // Case 3: EARLIEST first, zero offset second
    PubSubPosition zeroOffset = new ApacheKafkaOffsetPosition(0L);
    delta = PubSubUtil.computeOffsetDelta(partition, PubSubSymbolicPosition.EARLIEST, zeroOffset, consumerAdapter);
    assertEquals(delta, 0L, "Delta should be -0 = 0");

    // Case 4: Zero offset first, EARLIEST second
    delta = PubSubUtil.computeOffsetDelta(partition, zeroOffset, PubSubSymbolicPosition.EARLIEST, consumerAdapter);
    assertEquals(delta, 0L, "Delta should be 0");
  }

  @Test
  public void testComputeOffsetDeltaLatestAndNonSymbolic() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);

    // Mock the symbolic position resolution - return the symbolic position itself
    // This allows the symbolic logic in computeOffsetDelta to be triggered
    when(consumerAdapter.endPosition(partition)).thenReturn(PubSubSymbolicPosition.LATEST);

    // Case 1: LATEST first, non-symbolic second
    // LATEST triggers Case 4 logic: Long.MAX_VALUE - 100
    PubSubPosition nonSymbolic = new ApacheKafkaOffsetPosition(100L);
    long delta = PubSubUtil.computeOffsetDelta(partition, PubSubSymbolicPosition.LATEST, nonSymbolic, consumerAdapter);
    assertEquals(delta, Long.MAX_VALUE - 100L, "Delta should be Long.MAX_VALUE - 100 (LATEST symbolic logic)");

    // Case 2: Non-symbolic first, LATEST second
    // LATEST triggers Case 4 logic: 100 - Long.MAX_VALUE
    delta = PubSubUtil.computeOffsetDelta(partition, nonSymbolic, PubSubSymbolicPosition.LATEST, consumerAdapter);
    assertEquals(delta, 100L - Long.MAX_VALUE, "Delta should be 100 - Long.MAX_VALUE (LATEST symbolic logic)");

    // Case 3: LATEST first, zero offset second
    // LATEST triggers Case 4 logic: Long.MAX_VALUE - 0
    PubSubPosition zeroOffset = new ApacheKafkaOffsetPosition(0L);
    delta = PubSubUtil.computeOffsetDelta(partition, PubSubSymbolicPosition.LATEST, zeroOffset, consumerAdapter);
    assertEquals(delta, Long.MAX_VALUE, "Delta should be Long.MAX_VALUE - 0 = Long.MAX_VALUE (LATEST symbolic logic)");

    // Case 4: Zero offset first, LATEST second
    // LATEST triggers Case 4 logic: 0 - Long.MAX_VALUE
    delta = PubSubUtil.computeOffsetDelta(partition, zeroOffset, PubSubSymbolicPosition.LATEST, consumerAdapter);
    assertEquals(
        delta,
        -Long.MAX_VALUE,
        "Delta should be 0 - Long.MAX_VALUE = -Long.MAX_VALUE (LATEST symbolic logic)");
  }

  @Test
  public void testComputeOffsetDeltaEdgeCaseScenarios() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);

    // Mock the symbolic position resolution - return the symbolic positions themselves
    // This allows the symbolic logic in computeOffsetDelta to be triggered
    when(consumerAdapter.beginningPosition(partition)).thenReturn(PubSubSymbolicPosition.EARLIEST);
    when(consumerAdapter.endPosition(partition)).thenReturn(PubSubSymbolicPosition.LATEST);

    // Case 1: Large offset values
    PubSubPosition largePos1 = new ApacheKafkaOffsetPosition(Long.MAX_VALUE - 1000L);
    PubSubPosition largePos2 = new ApacheKafkaOffsetPosition(Long.MAX_VALUE - 2000L);
    long delta = PubSubUtil.computeOffsetDelta(partition, largePos1, largePos2, consumerAdapter);
    assertEquals(delta, 1000L, "Delta should handle large offset values correctly");

    // Case 2: Minimum offset values
    PubSubPosition minPos1 = new ApacheKafkaOffsetPosition(0L);
    PubSubPosition minPos2 = new ApacheKafkaOffsetPosition(1L);
    delta = PubSubUtil.computeOffsetDelta(partition, minPos1, minPos2, consumerAdapter);
    assertEquals(delta, -1L, "Delta should handle minimum offset values correctly");

    // Case 3: EARLIEST with large offset
    // EARLIEST resolves to 0L, so delta = 0 - 1000000 = -1000000
    PubSubPosition largeOffset = new ApacheKafkaOffsetPosition(1000000L);
    delta = PubSubUtil.computeOffsetDelta(partition, PubSubSymbolicPosition.EARLIEST, largeOffset, consumerAdapter);
    assertEquals(delta, -1000000L, "Delta should be 0 - 1000000 = -1000000 (resolved EARLIEST - large offset)");

    // Case 4: LATEST with large offset
    // LATEST triggers Case 4 logic: Long.MAX_VALUE - 1000000
    delta = PubSubUtil.computeOffsetDelta(partition, PubSubSymbolicPosition.LATEST, largeOffset, consumerAdapter);
    assertEquals(delta, Long.MAX_VALUE - 1000000L, "Delta should be Long.MAX_VALUE - 1000000 (LATEST symbolic logic)");
  }

  @Test
  public void testComputeOffsetDeltaUnsupportedPositionCombinations() {
    PubSubTopicPartition partition = mock(PubSubTopicPartition.class);
    PubSubConsumerAdapter consumerAdapter = mock(PubSubConsumerAdapter.class);

    // Create a mock position that's not handled by the method
    PubSubPosition unsupportedPosition = mock(PubSubPosition.class);
    when(unsupportedPosition.isSymbolic()).thenReturn(true);

    PubSubPosition normalPosition = new ApacheKafkaOffsetPosition(100L);

    // Case 1: Unsupported symbolic position with normal position
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.computeOffsetDelta(partition, unsupportedPosition, normalPosition, consumerAdapter));

    // Case 2: Normal position with unsupported symbolic position
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.computeOffsetDelta(partition, normalPosition, unsupportedPosition, consumerAdapter));

    // Case 3: Two unsupported positions
    PubSubPosition anotherUnsupportedPosition = mock(PubSubPosition.class);
    when(anotherUnsupportedPosition.isSymbolic()).thenReturn(true);
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil
            .computeOffsetDelta(partition, unsupportedPosition, anotherUnsupportedPosition, consumerAdapter));
  }

  // Helper method to create wire format string from position
  private String createWireFormatString(ApacheKafkaOffsetPosition position) {
    PubSubPositionWireFormat wireFormat = position.getPositionWireFormat();
    int typeId = wireFormat.getType();
    byte[] rawBytes = ByteUtils.extractByteArray(wireFormat.getRawBytes());
    String base64EncodedBytes = PubSubUtil.getBase64EncodedString(rawBytes);
    return typeId + ":" + base64EncodedBytes;
  }

  @Test
  public void testParsePositionWireFormatValidCases() {
    // Test with comprehensive set of offset values including edge cases from both original tests
    long[] testOffsets = { 0L, 1L, 100L, 12345L, 54321L, 9876543210L, Long.MAX_VALUE - 1000L, Long.MAX_VALUE };

    for (long offset: testOffsets) {
      // Step 1: Create ApacheKafkaOffsetPosition
      ApacheKafkaOffsetPosition originalPosition = new ApacheKafkaOffsetPosition(offset);

      // Step 2: Create string from the position and its type id
      String positionWireFormatString = createWireFormatString(originalPosition);

      // Step 3: Parse the position using the utility method
      PubSubPositionDeserializer deserializer = PubSubPositionDeserializer.DEFAULT_DESERIALIZER;
      PubSubPosition parsedPosition = PubSubUtil.parsePositionWireFormat(positionWireFormatString, deserializer);

      // Step 4: Basic validation - type and equality
      assertTrue(
          parsedPosition instanceof ApacheKafkaOffsetPosition,
          "Parsed position should be ApacheKafkaOffsetPosition");
      assertEquals(parsedPosition, originalPosition, "Parsed position should equal original for offset: " + offset);
      assertEquals(
          ((ApacheKafkaOffsetPosition) parsedPosition).getInternalOffset(),
          offset,
          "Parsed offset should match original offset: " + offset);

      // Step 5: Wire format component verification - ensure round-trip preserves internal structure
      PubSubPositionWireFormat originalWireFormat = originalPosition.getPositionWireFormat();
      PubSubPositionWireFormat parsedWireFormat = ((ApacheKafkaOffsetPosition) parsedPosition).getPositionWireFormat();

      assertEquals(
          parsedWireFormat.getType(),
          originalWireFormat.getType(),
          "Wire format type should be preserved for offset: " + offset);
      assertEquals(
          ByteUtils.extractByteArray(parsedWireFormat.getRawBytes()),
          ByteUtils.extractByteArray(originalWireFormat.getRawBytes()),
          "Wire format raw bytes should be preserved for offset: " + offset);
    }
  }

  @Test
  public void testParsePositionWireFormatInvalidInputs() {
    PubSubPositionDeserializer deserializer = PubSubPositionDeserializer.DEFAULT_DESERIALIZER;

    // Null and empty inputs
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat(null, deserializer));
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat("", deserializer));

    // Invalid format
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat("123abc", deserializer));
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat("1:2:3", deserializer));
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat(":", deserializer));

    // Invalid type ID
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.parsePositionWireFormat("abc:dGVzdA==", deserializer));
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat(":dGVzdA==", deserializer));

    // Invalid base64
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.parsePositionWireFormat("1:invalid_base64!", deserializer));
    expectThrows(IllegalArgumentException.class, () -> PubSubUtil.parsePositionWireFormat("1:", deserializer));
  }

  @Test
  public void testDeserializePositionWithOffsetFallback() {
    PubSubPositionDeserializer deserializer = PubSubPositionDeserializer.DEFAULT_DESERIALIZER;

    // Case 1: Null ByteBuffer - should return offset-based position
    PubSubPosition actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(null, 100L, deserializer);
    assertEquals(actualPosition.getNumericOffset(), 100L, "Null ByteBuffer should return offset-based position");

    // Case 2: Empty ByteBuffer - should return offset-based position
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(ByteBuffer.allocate(0), 200L, deserializer);
    assertEquals(actualPosition.getNumericOffset(), 200L, "Empty ByteBuffer should return offset-based position");

    // Case 3: Invalid ByteBuffer - should return offset-based position
    ByteBuffer invalidBuffer = ByteBuffer.wrap(new byte[] { 0x01, 0x02, 0x03 }); // Random invalid bytes
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(invalidBuffer, 300L, deserializer);
    assertEquals(actualPosition.getNumericOffset(), 300L, "Invalid ByteBuffer should return offset-based position");

    // Case 4: Valid ByteBuffer with position ahead of offset - should return deserialized position
    ByteBuffer validBuffer = ApacheKafkaOffsetPosition.of(400L).toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(validBuffer, 0L, deserializer);
    assertEquals(actualPosition.getNumericOffset(), 400L, "Valid ByteBuffer should return deserialized position");

    // Case 5: Valid ByteBuffer with position equal to offset - should return deserialized position
    ByteBuffer equalBuffer = ApacheKafkaOffsetPosition.of(500L).toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(equalBuffer, 500L, deserializer);
    assertEquals(
        actualPosition.getNumericOffset(),
        500L,
        "Position equal to offset should return deserialized position");

    // Case 6: Valid ByteBuffer with position behind offset - should return offset-based position
    ByteBuffer behindBuffer = ApacheKafkaOffsetPosition.of(100L).toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(behindBuffer, 200L, deserializer);
    assertEquals(actualPosition.getNumericOffset(), 200L, "Position behind offset should return offset-based position");

    // Case 7: ByteBuffer has EARLIEST symbolic position - should return EARLIEST as-is
    ByteBuffer earliestBuffer = PubSubSymbolicPosition.EARLIEST.toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(earliestBuffer, 100L, deserializer);
    assertEquals(actualPosition, PubSubSymbolicPosition.EARLIEST, "Should return EARLIEST symbolic position as-is");

    // Case 8: ByteBuffer has LATEST symbolic position - should return LATEST as-is
    ByteBuffer latestBuffer = PubSubSymbolicPosition.LATEST.toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(latestBuffer, 100L, deserializer);
    assertEquals(actualPosition, PubSubSymbolicPosition.LATEST, "Should return LATEST symbolic position as-is");

    // Case 9: Large offset values
    ByteBuffer largeBuffer = ApacheKafkaOffsetPosition.of(Long.MAX_VALUE - 1000L).toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(largeBuffer, 0L, deserializer);
    assertEquals(
        actualPosition.getNumericOffset(),
        Long.MAX_VALUE - 1000L,
        "Should handle large offset values correctly");

    // Case 10: Zero offset
    ByteBuffer zeroBuffer = ApacheKafkaOffsetPosition.of(0L).toWireFormatBuffer();
    actualPosition = PubSubUtil.deserializePositionWithOffsetFallback(zeroBuffer, 0L, deserializer);
    assertEquals(actualPosition.getNumericOffset(), 0L, "Should handle zero offset correctly");
  }
}
