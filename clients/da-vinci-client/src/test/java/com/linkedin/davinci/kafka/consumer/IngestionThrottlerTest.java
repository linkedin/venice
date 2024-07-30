package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
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

    IngestionThrottler throttler = new IngestionThrottler(true, serverConfig, () -> tasks, 10, TimeUnit.MILLISECONDS);
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
        new IngestionThrottler(false, serverConfig, () -> tasks, 10, TimeUnit.MILLISECONDS);
    tasks.put("current_version_bootstrapping_task", currentVersionBootstrappingTask);
    tasks.put("current_version_completed_task", currentVersionCompletedTask);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertFalse(
          throttlerForNonDaVinciClient.isUsingSpeedupThrottler(),
          "Shouldn't use speedup throttler as DaVinci is disabled");
    });

    throttlerForNonDaVinciClient.close();
  }
}
