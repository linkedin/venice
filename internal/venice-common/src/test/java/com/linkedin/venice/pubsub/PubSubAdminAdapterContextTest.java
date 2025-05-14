package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import org.testng.annotations.Test;


public class PubSubAdminAdapterContextTest {
  @Test
  public void testBuildWithAllRequiredFields() {
    VeniceProperties props = new VeniceProperties(new Properties());
    PubSubAdminAdapterContext context = new PubSubAdminAdapterContext.Builder().setVeniceProperties(props)
        .setPubSubBrokerAddress("localhost:1234")
        .setAdminClientName("admin-client")
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY)
        .setPubSubSecurityProtocol(PubSubSecurityProtocol.SSL)
        .setMetricsRepository(new MetricsRepository())
        .build();

    assertEquals(context.getPubSubBrokerAddress(), "localhost:1234");
    assertEquals(context.getAdminClientName().contains("admin-client"), true);
    assertEquals(context.getPubSubSecurityProtocol(), PubSubSecurityProtocol.SSL);
    assertNotNull(context.getMetricsRepository());
    assertNotNull(context.getVeniceProperties());
    assertNotNull(context.getPubSubTopicRepository());
    assertNotNull(context.getPubSubPositionTypeRegistry());
  }

  @Test
  public void testMissingVenicePropertiesThrows() {
    IllegalArgumentException exception = expectThrows(
        IllegalArgumentException.class,
        () -> new PubSubAdminAdapterContext.Builder().setPubSubTopicRepository(new PubSubTopicRepository())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY)
            .build());
    assertTrue(exception.getMessage().contains("Missing required properties"));
  }

  @Test
  public void testMissingTopicRepositoryThrows() {
    Properties rawProps = new Properties();
    rawProps.setProperty(ConfigKeys.PUBSUB_BROKER_ADDRESS, "localhost:1234");
    VeniceProperties props = new VeniceProperties(rawProps);
    IllegalArgumentException exception = expectThrows(
        IllegalArgumentException.class,
        () -> new PubSubAdminAdapterContext.Builder().setVeniceProperties(props)
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY)
            .build());
    assertTrue(exception.getMessage().contains("Missing required topic repository"), "Got: " + exception.getMessage());
  }

  @Test
  public void testMissingRegistryThrows() {
    Properties rawProps = new Properties();
    rawProps.setProperty(ConfigKeys.PUBSUB_BROKER_ADDRESS, "localhost:1234");
    VeniceProperties props = new VeniceProperties(rawProps);
    IllegalArgumentException exception = expectThrows(
        IllegalArgumentException.class,
        () -> new PubSubAdminAdapterContext.Builder().setVeniceProperties(props)
            .setPubSubTopicRepository(new PubSubTopicRepository())
            .build());
    assertTrue(
        exception.getMessage().contains("Missing required position type registry"),
        "Got: " + exception.getMessage());
  }

  @Test
  public void testDefaultSecurityProtocolAndBrokerFromProps() {
    Properties rawProps = new Properties();
    rawProps.setProperty("pubsub.broker.address", "localhost:4567");
    VeniceProperties props = new VeniceProperties(rawProps);

    PubSubAdminAdapterContext context = new PubSubAdminAdapterContext.Builder().setVeniceProperties(props)
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY)
        .build();

    assertEquals(context.getPubSubBrokerAddress(), "localhost:4567");
    assertEquals(context.getPubSubSecurityProtocol(), PubSubSecurityProtocol.PLAINTEXT); // default
    assertTrue(context.getAdminClientName().startsWith("ADMIN--"));
  }
}
