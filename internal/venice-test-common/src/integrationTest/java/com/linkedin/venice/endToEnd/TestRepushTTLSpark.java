package com.linkedin.venice.endToEnd;

/**
 * TTL repush integration tests using the Spark engine.
 */
public class TestRepushTTLSpark extends AbstractTestRepushTTL {
  @Override
  protected boolean useSpark() {
    return true;
  }
}
