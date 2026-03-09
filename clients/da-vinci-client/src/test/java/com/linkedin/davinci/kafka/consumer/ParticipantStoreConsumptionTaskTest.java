package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.stats.OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.stats.ParticipantStoreConsumptionStats;
import com.linkedin.util.clock.Time;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.utils.SleepStallingMockTime;
import com.linkedin.venice.utils.Utils;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.testng.annotations.Test;


public class ParticipantStoreConsumptionTaskTest {
  private static final long WAIT = 1 * Time.MS_PER_SECOND;
  private static final long EXPECTED_LAG = 100;
  private static final String CLIENT_CTOR_EXPLANATION =
      "The clientConstructor should not be called more than 3 times, since it failed twice and succeeded once, after which the result should be cached.";
  private SleepStallingMockTime mockTime = new SleepStallingMockTime();
  private long participantMessageConsumptionDelayMs = 1;
  private int iterations;

  @Test
  public void testOverallRunFlow() throws InterruptedException {
    StoreIngestionService storeIngestionService = mock(StoreIngestionService.class);
    ClusterInfoProvider clusterInfoProvider = mock(ClusterInfoProvider.class);
    ParticipantStoreConsumptionStats stats = mock(ParticipantStoreConsumptionStats.class);
    ClientConfig<ParticipantMessageValue> clientConfig = mock(ClientConfig.class);
    Function<ClientConfig<ParticipantMessageValue>, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> clientConstructor =
        mock(Function.class);
    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        mock(AvroSpecificStoreClient.class);
    doReturn(CompletableFuture.completedFuture(null)).when(client).get(any());

    String clusterName = "venice-0";
    Set<String> ingestingStoreVersions = new HashSet<>();
    String storeName = Utils.getUniqueString("participantStoreTest");
    String v1 = storeName + "_v1";
    String v2 = storeName + "_v2";
    ingestingStoreVersions.add(v1);
    ingestingStoreVersions.add(v2);
    doReturn(ingestingStoreVersions).when(storeIngestionService).getIngestingTopicsWithVersionStatusNotOnline();

    ParticipantStoreConsumptionTask task = new ParticipantStoreConsumptionTask(
        storeIngestionService,
        clusterInfoProvider,
        stats,
        clientConfig,
        participantMessageConsumptionDelayMs,
        null,
        clientConstructor,
        mockTime);
    CompletableFuture.runAsync(task);

    // Stalled on the first sleep, none of the mocks should be called yet.
    iterations = 0;
    verify(storeIngestionService, never()).getIngestingTopicsWithVersionStatusNotOnline();
    verify(clusterInfoProvider, never()).getVeniceCluster(any());
    verify(clientConstructor, never()).apply(any());
    verify(client, never()).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, never()).recordFailedInitialization();
    verify(stats, never()).recordKillPushJobFailedConsumption(any());
    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, never()).recordFailedKillPushJob(any());
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
    verify(storeIngestionService, never()).killConsumptionTask(any());

    // 1st sleep
    iterate();
    verify(storeIngestionService, timeout(WAIT).times(iterations)).getIngestingTopicsWithVersionStatusNotOnline();
    // N.B. This one still returns null
    verify(clusterInfoProvider, timeout(WAIT).times(iterations * 2)).getVeniceCluster(storeName);
    verify(clientConstructor, never()).apply(any());
    verify(client, never()).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, never()).recordFailedInitialization();
    verify(stats, never()).recordKillPushJobFailedConsumption(any());
    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, never()).recordFailedKillPushJob(any());
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
    verify(storeIngestionService, never()).killConsumptionTask(any());

    // 2nd sleep, after setting up the return of the clusterInfoProvider
    doReturn(clusterName).when(clusterInfoProvider).getVeniceCluster(storeName);
    iterate();
    verify(storeIngestionService, timeout(WAIT).times(iterations)).getIngestingTopicsWithVersionStatusNotOnline();
    verify(clusterInfoProvider, timeout(WAIT).times(iterations * 2)).getVeniceCluster(storeName);
    verify(clientConstructor, timeout(WAIT).times(2)).apply(any()); // N.B. This one still returns null
    verify(client, never()).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, timeout(WAIT).times(2)).recordFailedInitialization();
    verify(stats, timeout(WAIT).times(2)).recordKillPushJobFailedConsumption(storeName);
    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, never()).recordFailedKillPushJob(any());
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
    verify(storeIngestionService, never()).killConsumptionTask(any());

    // 3rd sleep, after setting up the return of the clientConstructor
    doReturn(client).when(clientConstructor).apply(any());
    iterate();
    verify(storeIngestionService, timeout(WAIT).times(iterations)).getIngestingTopicsWithVersionStatusNotOnline();
    verify(clusterInfoProvider, timeout(WAIT).times(iterations * 2)).getVeniceCluster(storeName);
    verify(clientConstructor, timeout(WAIT).times(3).description(CLIENT_CTOR_EXPLANATION)).apply(any());
    // N.B. This one still returns a completed CF containing null
    verify(client, timeout(WAIT).times((iterations - 2) * 2)).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, timeout(WAIT).times(2)).recordFailedInitialization();
    verify(stats, timeout(WAIT).times(2)).recordKillPushJobFailedConsumption(storeName);
    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, never()).recordFailedKillPushJob(any());
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
    verify(storeIngestionService, never()).killConsumptionTask(any());

    // 4th sleep, after making the client return something invalid
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.setMessageType(ParticipantMessageType.KILL_PUSH_JOB.getValue());
    key.setResourceName(v1);
    ParticipantMessageValue value = new ParticipantMessageValue();
    value.setMessageType(-1); // invalid
    doReturn(CompletableFuture.completedFuture(value)).when(client).get(key);
    iterate();
    verify(storeIngestionService, timeout(WAIT).times(iterations)).getIngestingTopicsWithVersionStatusNotOnline();
    verify(clusterInfoProvider, timeout(WAIT).times(iterations * 2)).getVeniceCluster(storeName);
    verify(clientConstructor, timeout(WAIT).times(3).description(CLIENT_CTOR_EXPLANATION)).apply(any());
    verify(client, timeout(WAIT).times((iterations - 2) * 2)).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, timeout(WAIT).times(2)).recordFailedInitialization();
    verify(stats, timeout(WAIT).times(2)).recordKillPushJobFailedConsumption(storeName);
    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, never()).recordFailedKillPushJob(any());
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
    verify(storeIngestionService, never()).killConsumptionTask(any());

    // 5th sleep, after making the client return something valid
    value = new ParticipantMessageValue();
    value.setMessageType(ParticipantMessageType.KILL_PUSH_JOB.getValue());
    KillPushJob killPushJobMessage = new KillPushJob();
    killPushJobMessage.setTimestamp(this.mockTime.getMilliseconds() - EXPECTED_LAG);
    value.setMessageUnion(killPushJobMessage);
    doReturn(CompletableFuture.completedFuture(value)).when(client).get(key);
    iterate();
    verify(storeIngestionService, timeout(WAIT).times(iterations)).getIngestingTopicsWithVersionStatusNotOnline();
    verify(clusterInfoProvider, timeout(WAIT).times(iterations * 2)).getVeniceCluster(storeName);
    verify(clientConstructor, timeout(WAIT).times(3).description(CLIENT_CTOR_EXPLANATION)).apply(any());
    verify(client, timeout(WAIT).times((iterations - 2) * 2)).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, timeout(WAIT).times(2)).recordFailedInitialization();
    verify(stats, timeout(WAIT).times(2)).recordKillPushJobFailedConsumption(storeName);
    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, timeout(WAIT).times(1)).recordFailedKillPushJob(storeName);
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
    verify(storeIngestionService, timeout(WAIT).times(1)).killConsumptionTask(v1);
    verify(storeIngestionService, never()).killConsumptionTask(v2);

    // 6th sleep, after making the client return something valid and succeeding to kill the job (finally!)
    doReturn(true).when(storeIngestionService).killConsumptionTask(v1);
    iterate();
    verify(storeIngestionService, timeout(WAIT).times(iterations)).getIngestingTopicsWithVersionStatusNotOnline();
    verify(clusterInfoProvider, timeout(WAIT).times(iterations * 2)).getVeniceCluster(storeName);
    verify(clientConstructor, timeout(WAIT).times(3).description(CLIENT_CTOR_EXPLANATION)).apply(any());
    verify(client, timeout(WAIT).times((iterations - 2) * 2)).get(any());
    verify(stats, timeout(WAIT).times(iterations + 1)).recordHeartbeat();
    verify(stats, timeout(WAIT).times(2)).recordFailedInitialization();
    verify(stats, timeout(WAIT).times(2)).recordKillPushJobFailedConsumption(storeName);
    verify(stats, timeout(WAIT).times(1)).recordKilledPushJobs(storeName);
    verify(stats, timeout(WAIT).times(1)).recordFailedKillPushJob(storeName);
    // +2 because we created the message two iterations ago...
    verify(stats, timeout(WAIT).times(1)).recordKillPushJobLatency(storeName, EXPECTED_LAG + 2);
    verify(storeIngestionService, timeout(WAIT).times(2)).killConsumptionTask(v1);
    verify(storeIngestionService, never()).killConsumptionTask(v2);

    task.close();
    verify(client, timeout(WAIT).times(1)).close();
  }

  /**
   * Verifies that when the outer catch block fires (e.g., {@code getIngestingTopicsWithVersionStatusNotOnline}
   * throws), {@code recordKillPushJobFailedConsumption} is called with {@code UNKNOWN_STORE_NAME} because
   * no store context is available at that level.
   */
  @Test
  public void testOuterCatchRecordsFailedConsumptionWithUnknownStore() {
    StoreIngestionService sis = mock(StoreIngestionService.class);
    ParticipantStoreConsumptionStats stats = mock(ParticipantStoreConsumptionStats.class);
    ClusterInfoProvider cip = mock(ClusterInfoProvider.class);
    ClientConfig<ParticipantMessageValue> clientConfig = mock(ClientConfig.class);
    Function<ClientConfig<ParticipantMessageValue>, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> clientCtor =
        mock(Function.class);
    SleepStallingMockTime time = new SleepStallingMockTime();

    doThrow(new RuntimeException("test: service unavailable")).when(sis).getIngestingTopicsWithVersionStatusNotOnline();

    ParticipantStoreConsumptionTask task =
        new ParticipantStoreConsumptionTask(sis, cip, stats, clientConfig, 1, null, clientCtor, time);
    CompletableFuture.runAsync(task);

    // Wait for the task to start and reach the sleep stall (heartbeat fires before sleep)
    verify(stats, timeout(WAIT).times(1)).recordHeartbeat();

    // Now advance time to unblock the sleep — the task will call getIngestingTopicsWithVersionStatusNotOnline
    // which throws, hitting the outer catch that records with UNKNOWN_STORE_NAME
    time.advanceTime(1);

    verify(stats, timeout(WAIT).times(1)).recordKillPushJobFailedConsumption(UNKNOWN_STORE_NAME);

    // Close the task before the negative assertions so no further loop iterations can fire.
    task.close();

    verify(stats, never()).recordKilledPushJobs(any());
    verify(stats, never()).recordFailedKillPushJob(any());
    verify(stats, never()).recordKillPushJobLatency(any(), anyDouble());
  }

  private void iterate() {
    iterations++;
    mockTime.advanceTime(participantMessageConsumptionDelayMs);
  }
}
