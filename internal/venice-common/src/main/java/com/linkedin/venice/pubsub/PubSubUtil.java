package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CLIENT_CONFIG_PREFIX;

import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;


public final class PubSubUtil {
  public static String getPubSubBrokerAddressOrFail(VeniceProperties properties) {
    String pubSubBrokerAddress = properties.getStringWithAlternative(PUBSUB_BROKER_ADDRESS, KAFKA_BOOTSTRAP_SERVERS);
    if (pubSubBrokerAddress == null) {
      throw new IllegalArgumentException(
          "Missing required broker address. Please specify either '" + KAFKA_BOOTSTRAP_SERVERS + "' or '"
              + PUBSUB_BROKER_ADDRESS + "' in the configuration.");
    }
    return pubSubBrokerAddress;
  }

  public static String getPubSubBrokerAddressOrFail(Properties properties) {
    String brokerAddress = properties.getProperty(PUBSUB_BROKER_ADDRESS);
    if (brokerAddress == null) {
      brokerAddress = properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    }
    if (brokerAddress == null) {
      throw new IllegalArgumentException(
          "Missing required broker address. Please specify either '" + KAFKA_BOOTSTRAP_SERVERS + "' or '"
              + PUBSUB_BROKER_ADDRESS + "' in the configuration.");
    }
    return brokerAddress;
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

  public static PubSubSecurityProtocol getPubSubSecurityProtocolOrDefault(VeniceProperties properties) {
    String securityProtocol =
        properties.getStringWithAlternative(KAFKA_SECURITY_PROTOCOL, PUBSUB_SECURITY_PROTOCOL, null);
    if (securityProtocol == null) {
      securityProtocol =
          properties.getString(KAFKA_CONFIG_PREFIX + KAFKA_SECURITY_PROTOCOL, PubSubSecurityProtocol.PLAINTEXT.name());
    }
    return PubSubSecurityProtocol.forName(securityProtocol);
  }
}
