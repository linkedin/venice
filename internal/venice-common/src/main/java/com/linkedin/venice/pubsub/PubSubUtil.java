package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CLIENT_CONFIG_PREFIX;

import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;


public final class PubSubUtil {
  public static String getPubSubBrokerAddress(Properties properties) {
    String brokerAddress = properties.getProperty(PUBSUB_BROKER_ADDRESS);
    if (brokerAddress == null) {
      brokerAddress = properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    }
    return brokerAddress;
  }

  public static String getPubSubBrokerAddress(VeniceProperties properties) {
    return properties.getStringWithAlternative(PUBSUB_BROKER_ADDRESS, KAFKA_BOOTSTRAP_SERVERS);
  }

  public static String getPubSubBrokerAddress(VeniceProperties properties, String defaultValue) {
    return properties.getStringWithAlternative(PUBSUB_BROKER_ADDRESS, KAFKA_BOOTSTRAP_SERVERS, defaultValue);
  }

  public static Properties addPubSubBrokerAddress(Properties properties, String brokerAddress) {
    properties.setProperty(PUBSUB_BROKER_ADDRESS, brokerAddress);
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, brokerAddress);
    return properties;
  }

  /**
   * Generates a standardized and unique client ID for PubSub clients.
   *
   * <p>
   * This ensures uniqueness in client IDs, preventing naming collisions that could cause
   * {@code InstanceAlreadyExistsException} during JMX metric registration. If multiple PubSub clients
   * share the same client ID, underlying client libraries (e.g., Kafka) may fail JMX registration,
   * resulting in runtime errors. By appending a timestamp, this method guarantees that each generated ID is unique.
   * </p>
   *
   * <p>
   * If the provided client name is null, it defaults to "venice-pubsub-client" followed by the host name.
   * If the broker address is null, it defaults to an empty string.
   * The generated client ID follows the format:
   * <pre>{@code clientName-brokerAddress-timestamp}</pre>
   * </p>
   *
   * @param clientName    The name of the client (can be null, defaults to "venice-pubsub-client" + host name).
   * @param brokerAddress The PubSub broker address (can be null, defaults to an empty string).
   * @return A unique client ID in the format: {@code clientName-brokerAddress-timestamp}.
   */
  public static String generatePubSubClientId(
      PubSubClientType pubSubClientType,
      String clientName,
      String brokerAddress) {
    String resolvedClientName = clientName != null ? clientName : "";
    String resolvedBrokerAddress = brokerAddress != null ? brokerAddress : "";

    return String.format(
        "%s-%s-%s-%s-%d",
        pubSubClientType,
        resolvedClientName,
        Utils.getHostName(),
        resolvedBrokerAddress,
        System.currentTimeMillis());
  }

  public static String getPubSubProducerConfigPrefix(String adapterConfigPrefix) {
    return getPubSubClientConfigPrefix(PubSubClientType.PRODUCER, adapterConfigPrefix);
  }

  public static String getPubSubConsumerConfigPrefix(String adapterConfigPrefix) {
    return getPubSubClientConfigPrefix(PubSubClientType.CONSUMER, adapterConfigPrefix);
  }

  public static String getPubSubAdminConfigPrefix(String adapterConfigPrefix) {
    return getPubSubClientConfigPrefix(PubSubClientType.ADMIN, adapterConfigPrefix);
  }

  private static String getPubSubClientConfigPrefix(PubSubClientType pubSubClientType, String adapterConfigPrefix) {
    if (adapterConfigPrefix == null || adapterConfigPrefix.length() > 1 && !adapterConfigPrefix.endsWith(".")) {
      throw new IllegalArgumentException("Adapter config prefix must not be null or empty and must end with '.'");
    }
    return PUBSUB_CLIENT_CONFIG_PREFIX + adapterConfigPrefix + pubSubClientType.name().toLowerCase() + ".";
  }
}
