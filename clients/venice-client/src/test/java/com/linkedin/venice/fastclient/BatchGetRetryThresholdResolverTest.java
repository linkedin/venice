package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.MultiKeyLongTailRetryThresholds;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class BatchGetRetryThresholdResolverTest {
  private static final String LOCAL_RANGE = "1-12:8,13-:30";
  private static final MultiKeyLongTailRetryThresholds SERVER_POLICY = MultiKeyLongTailRetryThresholds.parse("1-:50");

  @DataProvider(name = "precedence")
  public Object[][] precedence() {
    return new Object[][] {
        // fixed threshold, local range explicit, server policy, expected threshold (us) for 5 keys
        { 1000, false, SERVER_POLICY, 1000 }, // a fixed threshold beats the server policy
        { 0, false, SERVER_POLICY, 50_000 }, // otherwise the server policy wins
        { 0, true, SERVER_POLICY, 8_000 }, // unless the local range was set explicitly
        { 0, false, null, 8_000 } }; // no server policy falls back to the local range
  }

  @Test(dataProvider = "precedence")
  public void testEffectiveThresholdPrecedence(
      int fixedMicros,
      boolean rangeExplicit,
      MultiKeyLongTailRetryThresholds serverPolicy,
      int expectedMicros) {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(fixedMicros).when(clientConfig).getLongTailRetryThresholdForBatchGetInMicroSeconds();
    doReturn(rangeExplicit).when(clientConfig).isBatchGetRetryRangeExplicit();
    doReturn(LOCAL_RANGE).when(clientConfig).getLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds();
    BatchGetRetryThresholdResolver resolver = new BatchGetRetryThresholdResolver(clientConfig, () -> serverPolicy);
    assertEquals(resolver.getThresholdInMicroSeconds(5), expectedMicros);
  }
}
