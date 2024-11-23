package com.linkedin.venice.stats.dimensions;

public enum RequestValidationOutcome {
  VALID("valid"), INVALID_KEY_COUNT_LIMIT_EXCEEDED("invalid_key_count_limit_exceeded");

  private final String outcome;

  RequestValidationOutcome(String outcome) {
    this.outcome = outcome;
  }

  public String getOutcome() {
    return this.outcome;
  }
}
