package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class RequestValidationOutcomeTest {
  @Test
  public void testVeniceRequestValidationOutcome() {
    for (RequestValidationOutcome outcome: RequestValidationOutcome.values()) {
      switch (outcome) {
        case VALID:
          assertEquals(outcome.getOutcome(), "valid");
          break;
        case INVALID_KEY_COUNT_LIMIT_EXCEEDED:
          assertEquals(outcome.getOutcome(), "invalid_key_count_limit_exceeded");
          break;
        default:
          throw new IllegalArgumentException("Unknown outcome: " + outcome);
      }
    }
  }
}
