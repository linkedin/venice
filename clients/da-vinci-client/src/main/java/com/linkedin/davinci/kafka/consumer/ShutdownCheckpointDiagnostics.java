package com.linkedin.davinci.kafka.consumer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToLongFunction;
import org.apache.logging.log4j.Logger;


/**
 * Opt-in, shutdown-only observations. No storage reads, lifecycle decisions, or future cancellation.
 * A completed command is not proof of persistence; only the post-put hook supplies persisted values.
 */
final class ShutdownCheckpointDiagnostics {
  static final int PARTITIONS_PER_BATCH = 20;

  enum Stage {
    NOT_STARTED, DRAINING, COMMAND_SUBMITTED, COMPLETION_UNKNOWN, COMPLETED, SKIPPED
  }

  static final class Partition {
    private Stage stage = Stage.NOT_STARTED;
    private String persisted = "unavailable";
    private CompletableFuture<Void> command;

    synchronized void stage(Stage next) {
      stage = next;
    }

    synchronized void persisted(long heartbeat, long checkpoint) {
      persisted = heartbeat + "/" + checkpoint;
    }

    synchronized void observe(CompletableFuture<Void> command) {
      this.command = command;
    }

    synchronized Observation snapshot(int partition, Long heartbeat) {
      Stage observedStage = stage;
      if (command != null && command.isDone()) {
        observedStage = command.isCompletedExceptionally() ? Stage.COMPLETION_UNKNOWN : Stage.COMPLETED;
      }
      return new Observation(partition, observedStage, heartbeat, persisted);
    }
  }

  static final class Observation {
    final int partition;
    final Stage stage;
    final Long liveHeartbeat;
    final String persisted;

    Observation(int partition, Stage stage, Long liveHeartbeat, String persisted) {
      this.partition = partition;
      this.stage = stage;
      this.liveHeartbeat = liveHeartbeat;
      this.persisted = persisted;
    }

    @Override
    public String toString() {
      // Only fixed enums and numeric fields: entry size is bounded, including unavailable values.
      return "{partition=" + partition + ",stage=" + stage + ",liveHeartbeat="
          + (liveHeartbeat == null ? "unavailable" : liveHeartbeat) + ",observedPersistedHeartbeat/checkpoint="
          + persisted + "}";
    }
  }

  private final long startedNs = System.nanoTime();
  private final Map<Integer, Partition> partitions = new LinkedHashMap<>();

  ShutdownCheckpointDiagnostics(Map<Integer, PartitionConsumptionState> inventory) {
    inventory.keySet().forEach(partition -> partitions.put(partition, new Partition()));
  }

  Partition partition(int partition) {
    return partitions.get(partition);
  }

  void persisted(int partition, long heartbeat, long checkpoint) {
    Partition observation = partitions.get(partition);
    if (observation != null) {
      observation.persisted(heartbeat, checkpoint);
    }
  }

  List<Observation> snapshot(
      Map<Integer, PartitionConsumptionState> inventory,
      ToLongFunction<PartitionConsumptionState> heartbeat) {
    List<Observation> observations = new ArrayList<>(partitions.size());
    partitions.forEach((id, partition) -> {
      Long liveHeartbeat = null;
      try {
        liveHeartbeat = heartbeat.applyAsLong(inventory.get(id));
      } catch (RuntimeException ignored) {
        // A failed measurement must not prevent the summary or hide any other partition.
      }
      observations.add(partition.snapshot(id, liveHeartbeat));
    });
    return observations;
  }

  void log(Logger logger, String task, List<Observation> observations) {
    int completed = 0;
    int skipped = 0;
    List<Observation> unfinished = new ArrayList<>();
    for (Observation observation: observations) {
      if (observation.stage == Stage.COMPLETED) {
        completed++;
      } else if (observation.stage == Stage.SKIPPED) {
        skipped++;
      } else {
        unfinished.add(observation);
      }
    }
    logger.info(
        "Shutdown checkpoint summary: task={}, total={}, commandCompleted={}, skipped={}, unfinished={}, elapsedMs={}",
        task,
        observations.size(),
        completed,
        skipped,
        unfinished.size(),
        (System.nanoTime() - startedNs) / 1_000_000);
    int batches = (unfinished.size() + PARTITIONS_PER_BATCH - 1) / PARTITIONS_PER_BATCH;
    for (int start = 0; start < unfinished.size(); start += PARTITIONS_PER_BATCH) {
      logger.info(
          "Shutdown checkpoint unfinished: task={}, batch={}, batches={}, partitions={}",
          task,
          start / PARTITIONS_PER_BATCH + 1,
          batches,
          unfinished.subList(start, Math.min(start + PARTITIONS_PER_BATCH, unfinished.size())));
    }
  }
}
