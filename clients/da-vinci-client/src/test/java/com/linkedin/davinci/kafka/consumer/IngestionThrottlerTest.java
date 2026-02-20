package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class IngestionThrottlerTest {
  @Test
  public void throttlerSwitchTest() throws IOException {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(100l).when(serverConfig).getKafkaFetchQuotaRecordPerSecond();
    doReturn(60l).when(serverConfig).getKafkaFetchQuotaTimeWindow();
    doReturn(1024l).when(serverConfig).getKafkaFetchQuotaBytesPerSecond();
    doReturn(true).when(serverConfig).isDaVinciCurrentVersionBootstrappingSpeedupEnabled();
    doReturn(500l).when(serverConfig).getDaVinciCurrentVersionBootstrappingQuotaRecordsPerSecond();
    doReturn(10240l).when(serverConfig).getDaVinciCurrentVersionBootstrappingQuotaBytesPerSecond();
    mockThottlerRate(serverConfig);

    StoreIngestionTask nonCurrentVersionTask = mock(StoreIngestionTask.class);
    doReturn(false).when(nonCurrentVersionTask).isCurrentVersion();
    doReturn(false).when(nonCurrentVersionTask).hasAllPartitionReportedCompleted();

    StoreIngestionTask currentVersionBootstrappingTask = mock(StoreIngestionTask.class);
    doReturn(true).when(currentVersionBootstrappingTask).isCurrentVersion();
    doReturn(false).when(currentVersionBootstrappingTask).hasAllPartitionReportedCompleted();

    StoreIngestionTask currentVersionCompletedTask = mock(StoreIngestionTask.class);
    doReturn(true).when(currentVersionCompletedTask).isCurrentVersion();
    doReturn(true).when(currentVersionCompletedTask).hasAllPartitionReportedCompleted();

    ConcurrentHashMap<String, StoreIngestionTask> tasks = new ConcurrentHashMap<>();
    tasks.put("non_current_version_task", nonCurrentVersionTask);

    IngestionThrottler throttler =
        new IngestionThrottler(true, serverConfig, () -> tasks, 10, TimeUnit.MILLISECONDS, null);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertFalse(
          throttler.isUsingSpeedupThrottler(),
          "Shouldn't use speedup throttler when there is no current version bootstrapping");
    });
    // Add one bootstrapping current version.
    tasks.put("current_version_bootstrapping_task", currentVersionBootstrappingTask);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertTrue(
          throttler.isUsingSpeedupThrottler(),
          "Should use speedup throttler when there is some current version bootstrapping");
    });

    // Remove the bootstrapping current version and add one completed current version
    tasks.remove("current_version_bootstrapping_task");
    tasks.put("current_version_completed_task", currentVersionCompletedTask);

    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertFalse(
          throttler.isUsingSpeedupThrottler(),
          "Shouldn't use speedup throttler when there is no current version bootstrapping");
    });

    throttler.close();

    tasks.clear();
    IngestionThrottler throttlerForNonDaVinciClient =
        new IngestionThrottler(false, serverConfig, () -> tasks, 10, TimeUnit.MILLISECONDS, null);
    tasks.put("current_version_bootstrapping_task", currentVersionBootstrappingTask);
    tasks.put("current_version_completed_task", currentVersionCompletedTask);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertFalse(
          throttlerForNonDaVinciClient.isUsingSpeedupThrottler(),
          "Shouldn't use speedup throttler as DaVinci is disabled");
    });

    throttlerForNonDaVinciClient.close();
  }

  private static void mockThottlerRate(VeniceServerConfig serverConfig) {
    doReturn(-1).when(serverConfig).getSepRTLeaderQuotaRecordsPerSecond();
    doReturn(-1).when(serverConfig).getCurrentVersionAAWCLeaderQuotaRecordsPerSecond();
    doReturn(-1).when(serverConfig).getCurrentVersionSepRTLeaderQuotaRecordsPerSecond();
    doReturn(-1).when(serverConfig).getCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond();
    doReturn(-1).when(serverConfig).getNonCurrentVersionAAWCLeaderQuotaRecordsPerSecond();
    doReturn(-1).when(serverConfig).getNonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond();
  }

  @Test
  public void testDifferentThrottler() {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(100L).when(serverConfig).getKafkaFetchQuotaRecordPerSecond();
    doReturn(60L).when(serverConfig).getKafkaFetchQuotaTimeWindow();
    doReturn(1024L).when(serverConfig).getKafkaFetchQuotaBytesPerSecond();
    mockThottlerRate(serverConfig);
    IngestionThrottler ingestionThrottler =
        new IngestionThrottler(true, serverConfig, () -> Collections.emptyMap(), 10, TimeUnit.MILLISECONDS, null);
    EventThrottler throttlerForAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForCurrentVersionAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForCurrentVersionNonAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForNonCurrentVersionAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForNonCurrentVersionNonAAWCLeader = mock(EventThrottler.class);
    EventThrottler globalRecordThrottler = mock(EventThrottler.class);
    ingestionThrottler.setupGlobalRecordThrottler(globalRecordThrottler);
    ingestionThrottler.setupRecordThrottlerForPoolType(ConsumerPoolType.AA_WC_LEADER_POOL, throttlerForAAWCLeader);
    ingestionThrottler.setupRecordThrottlerForPoolType(
        ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
        throttlerForCurrentVersionAAWCLeader);
    ingestionThrottler.setupRecordThrottlerForPoolType(
        ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        throttlerForCurrentVersionNonAAWCLeader);
    ingestionThrottler.setupRecordThrottlerForPoolType(
        ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
        throttlerForNonCurrentVersionAAWCLeader);
    ingestionThrottler.setupRecordThrottlerForPoolType(
        ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        throttlerForNonCurrentVersionNonAAWCLeader);

    ingestionThrottler.maybeThrottleRecordRate(ConsumerPoolType.AA_WC_LEADER_POOL, 10);
    verify(throttlerForAAWCLeader).maybeThrottle(10);
    verify(globalRecordThrottler).maybeThrottle(10);

    ingestionThrottler.maybeThrottleRecordRate(ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL, 20);
    verify(throttlerForCurrentVersionAAWCLeader).maybeThrottle(20);
    verify(globalRecordThrottler).maybeThrottle(20);

    ingestionThrottler.maybeThrottleRecordRate(ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL, 30);
    verify(throttlerForCurrentVersionNonAAWCLeader).maybeThrottle(30);
    verify(globalRecordThrottler).maybeThrottle(30);

    ingestionThrottler.maybeThrottleRecordRate(ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL, 40);
    verify(throttlerForNonCurrentVersionAAWCLeader).maybeThrottle(40);
    verify(globalRecordThrottler).maybeThrottle(40);

    ingestionThrottler.maybeThrottleRecordRate(ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL, 50);
    verify(throttlerForNonCurrentVersionNonAAWCLeader).maybeThrottle(50);
    verify(globalRecordThrottler).maybeThrottle(50);
  }
}
