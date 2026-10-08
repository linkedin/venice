package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;


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

  public static Function<String, String> getRequiredKeyUrnLookup(VeniceProperties properties) {
    String keyUrn = properties.getString(PUB_SUB_ENCRYPTION_KEY_URN, (String) null);
    Function<String, String> lookup = getKeyUrnLookup(keyUrn);
    if (lookup == null || StringUtils.isBlank(keyUrn)) {
      throw new VeniceException("Encryption is enabled but " + PUB_SUB_ENCRYPTION_KEY_URN + " is missing or blank");
    }
    return lookup;
  }
}
