package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;

import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.function.Function;


public final class PubSubEncryptionUtils {
  private PubSubEncryptionUtils() {
  }

  /** One push job writes one store, so every topic uses the same key URN. */
  public static Function<String, String> getKeyUrnLookup(String keyUrn) {
    if (keyUrn == null || keyUrn.trim().isEmpty()) {
      return null;
    }
    String trimmedKeyUrn = keyUrn.trim();
    return storeName -> trimmedKeyUrn;
  }

  public static Function<String, String> getKeyUrnLookup(Properties properties) {
    return getKeyUrnLookup(properties.getProperty(PUB_SUB_ENCRYPTION_KEY_URN));
  }

  /**
   * Reads the compression dictionary from {@code topicName}'s Start Of Push message, resolving the V2 (li-crypt)
   * encryption key URN lookup from {@code props} ({@link #getKeyUrnLookup(Properties)}) instead of requiring the
   * caller to derive and pass it separately. Every VPJ caller threads {@link VenicePushJobConstants#PUB_SUB_ENCRYPTION_KEY_URN}
   * through the same {@code props} it passes here (see {@code VenicePushJob#getSourceDictionaryConsumerProperties}),
   * so deriving the lookup from {@code props} instead of a caller-supplied argument removes a redundant,
   * independently-computed copy of the same value at every call site.
   */
  public static ByteBuffer readDictionaryFromKafka(String topicName, VeniceProperties props) {
    return DictionaryUtils
        .readDictionaryFromKafkaWithEncryptionLookup(topicName, props, getKeyUrnLookup(props.toProperties()));
  }
}
