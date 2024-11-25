package com.linkedin.venice.stats.dimensions;

public enum RequestValidationOutcome {
  VALID, INVALID_KEY_COUNT_LIMIT_EXCEEDED;

  private final String outcome;

  RequestValidationOutcome() {
    this.outcome = name().toLowerCase();
  }

  public String getOutcome() {
    return this.outcome;
  }
}
