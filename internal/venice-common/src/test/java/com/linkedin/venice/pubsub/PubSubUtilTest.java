package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL_LEGACY;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL_LEGACY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.annotations.Test;


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
  public void testCompareWithNullPositionsThrowsException() {
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.comparePubSubPositions(null, PubSubSymbolicPosition.LATEST));
    expectThrows(
        IllegalArgumentException.class,
        () -> PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.EARLIEST, null));
  }

  @Test
  public void testCompareWithSymbolicPositions() {
    assertEquals(
        PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.EARLIEST, PubSubSymbolicPosition.EARLIEST),
        0);
    assertEquals(PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.LATEST, PubSubSymbolicPosition.LATEST), 0);
    assertTrue(PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.EARLIEST, PubSubSymbolicPosition.LATEST) < 0);
    assertTrue(PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.LATEST, PubSubSymbolicPosition.EARLIEST) > 0);
  }

  @Test
  public void testCompareWithSymbolicAndNumericPositions() {
    PubSubPosition numeric = mock(PubSubPosition.class);
    when(numeric.getNumericOffset()).thenReturn(100L);

    assertTrue(PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.EARLIEST, numeric) < 0);
    assertTrue(PubSubUtil.comparePubSubPositions(numeric, PubSubSymbolicPosition.EARLIEST) > 0);

    assertTrue(PubSubUtil.comparePubSubPositions(PubSubSymbolicPosition.LATEST, numeric) > 0);
    assertTrue(PubSubUtil.comparePubSubPositions(numeric, PubSubSymbolicPosition.LATEST) < 0);
  }

  @Test
  public void testCompareWithNumericPositions() {
    PubSubPosition pos1 = mock(PubSubPosition.class);
    PubSubPosition pos2 = mock(PubSubPosition.class);

    when(pos1.getNumericOffset()).thenReturn(50L);
    when(pos2.getNumericOffset()).thenReturn(100L);

    assertTrue(PubSubUtil.comparePubSubPositions(pos1, pos2) < 0);
    assertTrue(PubSubUtil.comparePubSubPositions(pos2, pos1) > 0);
    assertEquals(PubSubUtil.comparePubSubPositions(pos1, pos1), 0);
  }

  @Test
  public void testCalculateSeekOffset() {
    // Case 1: Inclusive = true, should return the same offset
    assertEquals(PubSubUtil.calculateSeekOffset(100L, true), 100L, "Inclusive seek should return the base offset");

    // Case 2: Inclusive = false, should return base offset + 1
    assertEquals(PubSubUtil.calculateSeekOffset(100L, false), 101L, "Exclusive seek should return base offset + 1");
  }
}
