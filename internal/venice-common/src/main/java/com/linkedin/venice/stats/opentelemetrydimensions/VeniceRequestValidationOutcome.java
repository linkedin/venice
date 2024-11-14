package com.linkedin.venice.stats.opentelemetrydimensions;

public enum VeniceRequestValidationOutcome {
  VALID("valid"), INVALID_KEY_COUNT_LIMIT_EXCEEDED("invalid_key_count_limit_exceeded");

  private final String outcome;

  VeniceRequestValidationOutcome(String outcome) {
    this.outcome = outcome;
  }

  public String getOutcome() {
    return this.outcome;
  }
}
