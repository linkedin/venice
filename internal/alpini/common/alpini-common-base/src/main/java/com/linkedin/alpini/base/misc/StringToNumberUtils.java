package com.linkedin.alpini.base.misc;

import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public enum StringToNumberUtils {
  SINGLETON;

  private static final Logger LOG = LogManager.getLogger(StringToNumberUtils.class);

  public static Optional<Integer> fromString(String contentLengthString) {
    if (contentLengthString == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Integer.parseUnsignedInt(contentLengthString));
    } catch (NumberFormatException ex) {
      LOG.warn("Caught a NumberFormatException when parsing {}", contentLengthString);
      return Optional.empty();
    }
  }
}
