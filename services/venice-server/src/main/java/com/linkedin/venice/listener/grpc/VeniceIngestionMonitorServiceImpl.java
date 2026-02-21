package com.linkedin.venice.listener.grpc;

import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.PartitionIngestionMonitor;
import com.linkedin.davinci.kafka.consumer.PartitionIngestionSnapshot;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.protocols.IngestionMonitorRequest;
import com.linkedin.venice.protocols.IngestionMonitorResponse;
import com.linkedin.venice.protocols.VeniceIngestionMonitorServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * gRPC service implementation for streaming ingestion metrics for a specific replica (topic-partition).
 *
 * <p>When a client connects, a {@link PartitionIngestionMonitor} is attached to the target partition's
 * {@link PartitionConsumptionState}. This activates zero-overhead metric collection on the ingestion hot path.
 * Metrics are periodically snapshotted and streamed back to the client. When the client disconnects,
 * the monitor is detached and all resources are cleaned up.
 *
 * <p>Only one monitoring session per partition is allowed. A second attempt returns {@code ALREADY_EXISTS}.
 */
public class VeniceIngestionMonitorServiceImpl
    extends VeniceIngestionMonitorServiceGrpc.VeniceIngestionMonitorServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceIngestionMonitorServiceImpl.class);
  private static final int MIN_INTERVAL_MS = 1000;
  private static final int DEFAULT_INTERVAL_MS = 5000;

  private final KafkaStoreIngestionService ingestionService;
  private final ConcurrentHashMap<String, ActiveSession> activeSessions = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(2, r -> new Thread(r, "ingestion-monitor-scheduler"));

  public VeniceIngestionMonitorServiceImpl(KafkaStoreIngestionService ingestionService) {
    this.ingestionService = ingestionService;
  }

  @Override
  public void monitorIngestion(
      IngestionMonitorRequest request,
      StreamObserver<IngestionMonitorResponse> responseObserver) {
    String versionTopic = request.getVersionTopic();
    int partition = request.getPartition();
    int intervalMs = request.getIntervalMs();

    if (versionTopic.isEmpty()) {
      responseObserver
          .onError(Status.INVALID_ARGUMENT.withDescription("version_topic is required").asRuntimeException());
      return;
    }

    if (intervalMs <= 0) {
      intervalMs = DEFAULT_INTERVAL_MS;
    } else if (intervalMs < MIN_INTERVAL_MS) {
      intervalMs = MIN_INTERVAL_MS;
    }

    String sessionKey = versionTopic + "_" + partition;

    // Check for duplicate session
    if (activeSessions.containsKey(sessionKey)) {
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription("A monitoring session is already active for " + sessionKey)
              .asRuntimeException());
      return;
    }

    // Find the ingestion task
    StoreIngestionTask sit = ingestionService.getStoreIngestionTask(versionTopic);
    if (sit == null) {
      responseObserver.onError(
          Status.NOT_FOUND.withDescription("No ingestion task found for version topic: " + versionTopic)
              .asRuntimeException());
      return;
    }

    // Find the PCS
    PartitionConsumptionState pcs = sit.getPartitionConsumptionState(partition);
    if (pcs == null) {
      responseObserver.onError(
          Status.NOT_FOUND
              .withDescription("No partition consumption state for partition " + partition + " of " + versionTopic)
              .asRuntimeException());
      return;
    }

    // Create and attach the monitor
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();
    pcs.setIngestionMonitor(monitor);

    // Register the session
    ServerCallStreamObserver<IngestionMonitorResponse> serverObserver =
        (ServerCallStreamObserver<IngestionMonitorResponse>) responseObserver;

    // Schedule periodic emission
    final long[] lastSnapshotTimeMs = { System.currentTimeMillis() };
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
      try {
        // Check if PCS is still alive
        PartitionConsumptionState currentPcs = sit.getPartitionConsumptionState(partition);
        if (currentPcs == null || currentPcs != pcs) {
          LOGGER.info("PCS gone for {}, ending monitoring session", sessionKey);
          cleanup(sessionKey, pcs);
          serverObserver.onCompleted();
          return;
        }

        if (serverObserver.isCancelled()) {
          cleanup(sessionKey, pcs);
          return;
        }

        long now = System.currentTimeMillis();
        long elapsedMs = now - lastSnapshotTimeMs[0];
        lastSnapshotTimeMs[0] = now;

        PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(elapsedMs);

        IngestionMonitorResponse.Builder builder = IngestionMonitorResponse.newBuilder()
            .setTimestampMs(now)
            .setLeaderFollowerState(pcs.getLeaderFollowerState().name())
            .setIsHybrid(pcs.isHybrid())
            .setRecordsPolledPerSec(snapshot.getRecordsConsumedPerSec())
            .setBytesPolledPerSec(snapshot.getBytesConsumedPerSec())
            .setConsumedRecordE2EProcessingLatencyAvgMs(snapshot.getE2eProcessingLatencyAvgMs())
            .setLeaderPreprocessingLatencyAvgMs(snapshot.getLeaderPreprocessingLatencyAvgMs())
            .setLeaderProduceLatencyAvgMs(snapshot.getLeaderProduceLatencyAvgMs())
            .setLeaderProducerCompletionLatencyAvgMs(snapshot.getLeaderCompletionLatencyAvgMs())
            .setLeaderProducerCallbackLatencyAvgMs(snapshot.getLeaderCallbackLatencyAvgMs())
            .setLeaderRecordsProducedPerSec(snapshot.getLeaderRecordsProducedPerSec())
            .setLeaderBytesProducedPerSec(snapshot.getLeaderBytesProducedPerSec())
            .setStorageEnginePutLatencyAvgMs(snapshot.getStoragePutLatencyAvgMs())
            .setLeaderValueBytesLookupLatencyAvgMs(snapshot.getValueLookupLatencyAvgMs())
            .setLeaderRmdLookupLatencyAvgMs(snapshot.getRmdLookupLatencyAvgMs());

        // Elapsed time since last record
        long latestConsumedTimestamp = pcs.getLatestMessageConsumedTimestampInMs();
        if (latestConsumedTimestamp > 0) {
          builder.setElapsedTimeSinceLastRecordMs(now - latestConsumedTimestamp);
        }

        serverObserver.onNext(builder.build());
      } catch (Exception e) {
        LOGGER.error("Error in monitoring session for {}", sessionKey, e);
        cleanup(sessionKey, pcs);
        try {
          serverObserver
              .onError(Status.INTERNAL.withDescription("Monitoring error: " + e.getMessage()).asRuntimeException());
        } catch (Exception ignored) {
          // Observer may already be closed
        }
      }
    }, 0, intervalMs, TimeUnit.MILLISECONDS);

    ActiveSession session = new ActiveSession(future, pcs, monitor);
    activeSessions.put(sessionKey, session);

    // Handle client disconnect
    serverObserver.setOnCancelHandler(() -> {
      LOGGER.info("Client disconnected from monitoring session for {}", sessionKey);
      cleanup(sessionKey, pcs);
    });

    LOGGER.info("Started ingestion monitoring session for {} with interval {}ms", sessionKey, intervalMs);
  }

  private void cleanup(String sessionKey, PartitionConsumptionState pcs) {
    ActiveSession session = activeSessions.remove(sessionKey);
    if (session != null) {
      session.future.cancel(false);
      // Detach monitor from PCS (only if it's still our monitor)
      if (pcs.getIngestionMonitor() == session.monitor) {
        pcs.setIngestionMonitor(null);
      }
      LOGGER.info("Cleaned up monitoring session for {}", sessionKey);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  private static class ActiveSession {
    final ScheduledFuture<?> future;
    final PartitionConsumptionState pcs;
    final PartitionIngestionMonitor monitor;

    ActiveSession(ScheduledFuture<?> future, PartitionConsumptionState pcs, PartitionIngestionMonitor monitor) {
      this.future = future;
      this.pcs = pcs;
      this.monitor = monitor;
    }
  }
}
