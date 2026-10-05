package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;

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
}
