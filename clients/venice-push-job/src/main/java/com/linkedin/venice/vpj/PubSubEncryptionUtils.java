package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;

import java.util.Properties;
import java.util.function.Function;


public final class PubSubEncryptionUtils {
  private PubSubEncryptionUtils() {
  }

  /**
   * Builds the topic-to-URN lookup that {@code VeniceWriterFactory} expects. The returned function ignores
   * its argument because a push job writes exactly one store; see
   * {@link VenicePushJobConstants#PUB_SUB_ENCRYPTION_KEY_URN} for the full rationale.
   *
   * @return the lookup, or {@code null} when no URN is configured, meaning "not encrypted"
   */
  public static Function<String, String> getKeyUrnLookup(String keyUrn) {
    if (keyUrn == null || keyUrn.trim().isEmpty()) {
      return null;
    }
    String trimmedKeyUrn = keyUrn.trim();
    return storeName -> trimmedKeyUrn;
  }

  /**
   * Data-writer-task variant of {@link #getKeyUrnLookup(String)}, reading the URN the driver threaded
   * through the job configuration.
   */
  public static Function<String, String> getKeyUrnLookup(Properties properties) {
    return getKeyUrnLookup(properties.getProperty(PUB_SUB_ENCRYPTION_KEY_URN));
  }
}
