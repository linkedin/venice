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

  @Test
  public void testSpeedupThrottlerSkipsDaVinciFutureSlotTopic() throws IOException {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(100l).when(serverConfig).getKafkaFetchQuotaRecordPerSecond();
    doReturn(60l).when(serverConfig).getKafkaFetchQuotaTimeWindow();
    doReturn(1024l).when(serverConfig).getKafkaFetchQuotaBytesPerSecond();
    doReturn(true).when(serverConfig).isDaVinciCurrentVersionBootstrappingSpeedupEnabled();
    doReturn(500l).when(serverConfig).getDaVinciCurrentVersionBootstrappingQuotaRecordsPerSecond();
    doReturn(10240l).when(serverConfig).getDaVinciCurrentVersionBootstrappingQuotaBytesPerSecond();
    mockThottlerRate(serverConfig);

    // task.isCurrentVersion() returns true (controller view says it's current — e.g. just after
    // a deferred-swap promotion). But DVC is still treating it as its future-version slot, so we
    // pass it through the future-slot predicate. Speedup throttler must not activate.
    StoreIngestionTask futureSlotTask = mock(StoreIngestionTask.class);
    doReturn(true).when(futureSlotTask).isCurrentVersion();
    doReturn(false).when(futureSlotTask).hasAllPartitionReportedCompleted();

    String futureSlotTopic = "store_v5";
    ConcurrentHashMap<String, StoreIngestionTask> tasks = new ConcurrentHashMap<>();
    tasks.put(futureSlotTopic, futureSlotTask);

    java.util.Set<String> daVinciFutureSlotTopics = ConcurrentHashMap.newKeySet();
    daVinciFutureSlotTopics.add(futureSlotTopic);

    IngestionThrottler throttler = new IngestionThrottler(
        true,
        serverConfig,
        () -> tasks,
        daVinciFutureSlotTopics::contains,
        10,
        TimeUnit.MILLISECONDS,
        null);

    // Wait long enough that the periodic check could have flipped the throttler — and assert it
    // didn't.
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> {
      assertFalse(
          throttler.isUsingSpeedupThrottler(),
          "Speedup throttler must not activate for a DVC future-slot topic, even when "
              + "isCurrentVersion() returns true");
    });

    // Once the topic leaves the future slot (DVC swapped it to current), the speedup throttler
    // should activate as before.
    daVinciFutureSlotTopics.remove(futureSlotTopic);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertTrue(
          throttler.isUsingSpeedupThrottler(),
          "Speedup throttler should activate once the topic leaves DVC's future slot");
    });

    throttler.close();
  }

  private static void mockThottlerRate(VeniceServerConfig serverConfig) {
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
    EventThrottler throttlerForCurrentVersionAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForCurrentVersionNonAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForNonCurrentVersionAAWCLeader = mock(EventThrottler.class);
    EventThrottler throttlerForNonCurrentVersionNonAAWCLeader = mock(EventThrottler.class);
    EventThrottler globalRecordThrottler = mock(EventThrottler.class);
    ingestionThrottler.setupGlobalRecordThrottler(globalRecordThrottler);
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
