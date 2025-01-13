package com.linkedin.venice.stats;

import com.linkedin.venice.utils.VeniceEnumValue;


public enum VeniceOpenTelemetryMetricNamingFormat implements VeniceEnumValue {
  /**
   * Default format if not configured, names are defined as per this.
   * should use snake case as per https://opentelemetry.io/docs/specs/semconv/general/attribute-naming/
   * For example: http.response.status_code
   */
  SNAKE_CASE(0),
  /**
   * Alternate format for attribute names. If configured, defined names in snake_case will be
   * transformed to either one of below formats.
   *
   * camel case: For example, http.response.statusCode
   * pascal case: For example, Http.Response.StatusCode
   */
  CAMEL_CASE(1), PASCAL_CASE(2);

  private final int value;

  VeniceOpenTelemetryMetricNamingFormat(int value) {
    this.value = value;
  }

  public static final int SIZE = values().length;

  @Override
  public int getValue() {
    return value;
  }

  /**
   * validate whether the input name is defined as a valid {@link VeniceOpenTelemetryMetricNamingFormat#SNAKE_CASE}
   */
  public static void validateMetricName(String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Metric name cannot be null or empty. Input name: " + name);
    }
    if (name.contains(" ")) {
      throw new IllegalArgumentException("Metric name cannot contain spaces. Input name: " + name);
    }
    // name should not contain any capital or special characters except for underscore and dot
    if (!name.matches("^[a-z0-9_.]*$")) {
      throw new IllegalArgumentException(
          "Metric name can only contain lowercase alphabets, numbers, underscore and dot. Input name: " + name);
    }
  }

  public static String transformMetricName(String input, VeniceOpenTelemetryMetricNamingFormat metricFormat) {
    if (metricFormat == SNAKE_CASE) {
      // no transformation needed as it should be defined in snake case by default
      validateMetricName(input);
      return input;
    }
    String[] words = input.split("\\.");
    for (int i = 0; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        String[] partWords = words[i].split("_");
        for (int j = 0; j < partWords.length; j++) {
          if (metricFormat == PASCAL_CASE || j > 0) {
            // either pascal case or camel case except for the first word
            partWords[j] = capitalizeFirstLetter(partWords[j]);
          }
        }
        StringBuilder sb = new StringBuilder();
        for (String partWord: partWords) {
          sb.append(partWord);
        }
        words[i] = sb.toString();
      }
    }
    StringBuilder finalName = new StringBuilder();
    for (String word: words) {
      finalName.append(word);
      finalName.append(".");
    }
    // remove the last dot
    if (finalName.length() > 0) {
      finalName.deleteCharAt(finalName.length() - 1);
    }
    return finalName.toString();
  }

  private static String capitalizeFirstLetter(String word) {
    if (word.isEmpty()) {
      return word;
    }
    return Character.toUpperCase(word.charAt(0)) + word.substring(1);
  }

  public static VeniceOpenTelemetryMetricNamingFormat getDefaultFormat() {
    return SNAKE_CASE;
  }
}
