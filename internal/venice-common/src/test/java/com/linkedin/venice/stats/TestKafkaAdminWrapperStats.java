package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestKafkaAdminWrapperStats {
  @Test
  public void testSingleton() {
    final MetricsRepository repo1 = new MetricsRepository();
    final MetricsRepository repo2 = new MetricsRepository();
    final String prefix1 = "prefix1";
    final String prefix2 = "prefix2";

    KafkaAdminWrapperStats statsForRepo1AndPrefix1 = KafkaAdminWrapperStats.getInstance(repo1, prefix1);
    Assert.assertEquals(
        KafkaAdminWrapperStats.getInstance(repo1, prefix1),
        statsForRepo1AndPrefix1,
        "KafkaAdminWrapperStats.getInstance should return the same instance with the same MetricsRepository and stat prefix params");
    Assert.assertNotEquals(
        KafkaAdminWrapperStats.getInstance(repo1, prefix2),
        statsForRepo1AndPrefix1,
        "KafkaAdminWrapperStats.getInstance should return a different instance with the same MetricsRepository and different prefix params");
    Assert.assertNotEquals(
        KafkaAdminWrapperStats.getInstance(repo2, prefix1),
        statsForRepo1AndPrefix1,
        "KafkaAdminWrapperStats.getInstance should return a different instance with the different MetricsRepository and same prefix params");
    Assert.assertNotEquals(
        KafkaAdminWrapperStats.getInstance(repo2, prefix2),
        statsForRepo1AndPrefix1,
        "KafkaAdminWrapperStats.getInstance should return a different instance with the different MetricsRepository and different prefix params");
  }
}
