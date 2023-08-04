package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPubSubAdminWrapperStats {
  @Test
  public void testSingleton() {
    final MetricsRepository repo1 = new MetricsRepository();
    final MetricsRepository repo2 = new MetricsRepository();
    final String prefix1 = "prefix1";
    final String prefix2 = "prefix2";

    PubSubAdminWrapperStats statsForRepo1AndPrefix1 = PubSubAdminWrapperStats.getInstance(repo1, prefix1);
    Assert.assertEquals(
        PubSubAdminWrapperStats.getInstance(repo1, prefix1),
        statsForRepo1AndPrefix1,
        "PubSubAdminWrapperStats.getInstance should return the same instance with the same MetricsRepository and stat prefix params");
    Assert.assertNotEquals(
        PubSubAdminWrapperStats.getInstance(repo1, prefix2),
        statsForRepo1AndPrefix1,
        "PubSubAdminWrapperStats.getInstance should return a different instance with the same MetricsRepository and different prefix params");
    Assert.assertNotEquals(
        PubSubAdminWrapperStats.getInstance(repo2, prefix1),
        statsForRepo1AndPrefix1,
        "PubSubAdminWrapperStats.getInstance should return a different instance with the different MetricsRepository and same prefix params");
    Assert.assertNotEquals(
        PubSubAdminWrapperStats.getInstance(repo2, prefix2),
        statsForRepo1AndPrefix1,
        "PubSubAdminWrapperStats.getInstance should return a different instance with the different MetricsRepository and different prefix params");
  }
}
