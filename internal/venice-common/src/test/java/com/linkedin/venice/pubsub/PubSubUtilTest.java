package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

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
  public void testGetPubSubBrokerAddressWithDefault() {
    Properties props = new Properties();
    VeniceProperties veniceProps = new VeniceProperties(props);
    String result = PubSubUtil.getPubSubBrokerAddress(veniceProps, "default://broker");
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
}
