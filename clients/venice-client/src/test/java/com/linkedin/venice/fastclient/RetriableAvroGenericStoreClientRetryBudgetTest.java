package com.linkedin.venice.fastclient;

public class RetriableAvroGenericStoreClientRetryBudgetTest extends RetriableAvroGenericStoreClientTest {
  @Override
  protected boolean isRetryBudgetEnabled() {
    return true;
  }

}
