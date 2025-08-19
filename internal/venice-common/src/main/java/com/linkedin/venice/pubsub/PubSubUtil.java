package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL_LEGACY;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL_LEGACY;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CLIENT_CONFIG_PREFIX;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
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

  public static String getPubSubBrokerAddressWithDefault(VeniceProperties properties, String defaultValue) {
    return properties.getStringWithAlternative(PUBSUB_BROKER_ADDRESS, KAFKA_BOOTSTRAP_SERVERS, defaultValue);
  }

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
        "%s-%s-from-%s-to-%s-%d",
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

  /**
   * TODO: Enforce explicit configuration of the PubSub security protocol in all components.
   * Avoid defaulting to PubSubSecurityProtocol.PLAINTEXT. If the protocol is not explicitly
   * defined via configuration, fail fast during startup to prevent silent misconfigurations.
   *
   * @param properties VeniceProperties containing configuration keys
   * @return the resolved PubSubSecurityProtocol
   */
  public static PubSubSecurityProtocol getPubSubSecurityProtocolOrDefault(VeniceProperties properties) {
    String securityProtocol =
        properties.getStringWithAlternative(KAFKA_SECURITY_PROTOCOL_LEGACY, PUBSUB_SECURITY_PROTOCOL_LEGACY, null);
    if (securityProtocol == null) {
      securityProtocol = properties.getString(PUBSUB_SECURITY_PROTOCOL, PubSubSecurityProtocol.PLAINTEXT.name());
    }
    return PubSubSecurityProtocol.forName(securityProtocol);
  }

  /**
   * Returns the {@link PubSubSecurityProtocol} configured in the given {@link Properties}, falling back
   * to PLAINTEXT if no value is found.
   *
   * @param properties the Java {@link Properties} object to extract the security protocol from
   * @return the resolved {@link PubSubSecurityProtocol}, or PLAINTEXT if not specified
   */
  public static PubSubSecurityProtocol getPubSubSecurityProtocolOrDefault(Properties properties) {
    String securityProtocol = properties.getProperty(KAFKA_SECURITY_PROTOCOL_LEGACY);
    if (securityProtocol == null) {
      securityProtocol = properties.getProperty(PUBSUB_SECURITY_PROTOCOL_LEGACY);
    }
    if (securityProtocol == null) {
      securityProtocol = properties.getProperty(PUBSUB_SECURITY_PROTOCOL, PubSubSecurityProtocol.PLAINTEXT.name());
    }
    return PubSubSecurityProtocol.forName(securityProtocol);
  }

  /**
   * Checks if the provided {@link PubSubSecurityProtocol} requires SSL.
   *
   * @param pubSubSecurityProtocol the security protocol to check
   * @return {@code true} if the protocol uses SSL (either SSL or SASL_SSL), {@code false} otherwise
   */
  public static boolean isPubSubSslProtocol(PubSubSecurityProtocol pubSubSecurityProtocol) {
    return pubSubSecurityProtocol == PubSubSecurityProtocol.SSL
        || pubSubSecurityProtocol == PubSubSecurityProtocol.SASL_SSL;
  }

  /**
   * Checks if the given security protocol name corresponds to a protocol that requires SSL.
   *
   * @param pubSubSecurityProtocol the name of the security protocol (case-insensitive)
   * @return {@code true} if the protocol uses SSL, {@code false} otherwise
   * @throws IllegalArgumentException if the name does not correspond to a valid {@link PubSubSecurityProtocol}
   */
  public static boolean isPubSubSslProtocol(String pubSubSecurityProtocol) {
    if (pubSubSecurityProtocol == null) {
      return false;
    }
    try {
      return isPubSubSslProtocol(PubSubSecurityProtocol.forName(pubSubSecurityProtocol));
    } catch (IllegalArgumentException e) {
      return false; // or rethrow if desired
    }
  }

  /**
   * Compares two {@link PubSubPosition} instances by their symbolic or numeric ordering.
   *
   * <p>This method defines the following ordering:
   * EARLIEST &lt; numeric offsets &lt; LATEST</p>
   *
   * @param position1 The first position.
   * @param position2 The second position.
   * @return A negative integer, zero, or a positive integer if position1 is less than,
   *         equal to, or greater than position2, respectively.
   * @throws IllegalArgumentException if either position is {@code null}.
   */
  public static int comparePubSubPositions(PubSubPosition position1, PubSubPosition position2) {
    if (position1 == null || position2 == null) {
      throw new IllegalArgumentException("Positions cannot be null");
    }

    if ((position1 == PubSubSymbolicPosition.EARLIEST && position2 == PubSubSymbolicPosition.EARLIEST)
        || (position1 == PubSubSymbolicPosition.LATEST && position2 == PubSubSymbolicPosition.LATEST)) {
      return 0;
    }

    if (position1 == PubSubSymbolicPosition.EARLIEST) {
      return -1;
    }
    if (position1 == PubSubSymbolicPosition.LATEST) {
      return 1;
    }
    if (position2 == PubSubSymbolicPosition.EARLIEST) {
      return 1;
    }
    if (position2 == PubSubSymbolicPosition.LATEST) {
      return -1;
    }

    return Long.compare(position1.getNumericOffset(), position2.getNumericOffset());
  }

  @FunctionalInterface
  public interface OffsetExtractor<T extends PubSubPosition> {
    long getInternalOffset(T position);
  }

  public static <T extends PubSubPosition> long computeOffsetDelta(
      PubSubTopicPartition partition,
      PubSubPosition position1,
      PubSubPosition position2,
      PubSubConsumerAdapter consumerAdapter,
      Class<T> concretePositionClass,
      OffsetExtractor<T> offsetExtractor) {

    if (position1 == null || position2 == null) {
      throw new IllegalArgumentException("Positions cannot be null");
    }

    PubSubPosition resolved1 = resolveSymbolicPosition(partition, position1, consumerAdapter);
    PubSubPosition resolved2 = resolveSymbolicPosition(partition, position2, consumerAdapter);

    // Case 1: Both resolved to concrete type
    if (concretePositionClass.isInstance(resolved1) && concretePositionClass.isInstance(resolved2)) {
      long offset1 = offsetExtractor.getInternalOffset(concretePositionClass.cast(resolved1));
      long offset2 = offsetExtractor.getInternalOffset(concretePositionClass.cast(resolved2));
      return offset1 - offset2;
    }

    // Case 2: Equal symbolic positions
    if (resolved1 == resolved2
        && (resolved1 == PubSubSymbolicPosition.EARLIEST || resolved1 == PubSubSymbolicPosition.LATEST)) {
      return 0L;
    }

    // Case 3: One is EARLIEST, one is concrete
    if (resolved1 == PubSubSymbolicPosition.EARLIEST && concretePositionClass.isInstance(resolved2)) {
      return -offsetExtractor.getInternalOffset(concretePositionClass.cast(resolved2));
    }
    if (resolved2 == PubSubSymbolicPosition.EARLIEST && concretePositionClass.isInstance(resolved1)) {
      return offsetExtractor.getInternalOffset(concretePositionClass.cast(resolved1));
    }

    // Case 4: One is LATEST, one is concrete
    if (resolved1 == PubSubSymbolicPosition.LATEST && concretePositionClass.isInstance(resolved2)) {
      return Long.MAX_VALUE - offsetExtractor.getInternalOffset(concretePositionClass.cast(resolved2));
    }
    if (resolved2 == PubSubSymbolicPosition.LATEST && concretePositionClass.isInstance(resolved1)) {
      return offsetExtractor.getInternalOffset(concretePositionClass.cast(resolved1)) - Long.MAX_VALUE;
    }

    throw new IllegalArgumentException(
        "Unsupported position types: " + resolved1.getClass().getName() + " vs " + resolved2.getClass().getName());
  }

  private static PubSubPosition resolveSymbolicPosition(
      PubSubTopicPartition partition,
      PubSubPosition position,
      PubSubConsumerAdapter consumerAdapter) {
    if (position == PubSubSymbolicPosition.EARLIEST) {
      return consumerAdapter.beginningPosition(partition);
    } else if (position == PubSubSymbolicPosition.LATEST) {
      return consumerAdapter.endPosition(partition);
    }
    return position;
  }

  /**
   * Calculates the seek offset based on the base offset and inclusiveness flag.
   *
   * @param baseOffset the base offset to calculate from
   * @param isInclusive if true, returns the base offset; if false, returns base offset + 1
   * @return the calculated seek offset
   */
  public static long calculateSeekOffset(long baseOffset, boolean isInclusive) {
    return isInclusive ? baseOffset : baseOffset + 1;
  }
}
