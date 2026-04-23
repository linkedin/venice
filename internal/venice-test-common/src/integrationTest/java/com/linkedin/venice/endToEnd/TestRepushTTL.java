package com.linkedin.venice.endToEnd;

/**
 * TTL repush integration tests using the MR (MapReduce) engine.
 */
public class TestRepushTTL extends AbstractTestRepushTTL {
  @Override
  protected boolean useSpark() {
    return false;
  }
}
