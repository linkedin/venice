package com.linkedin.venice.listener;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.netty.util.AttributeKey;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.atomic.AtomicInteger;


public class VeniceServerNettyStats extends AbstractVeniceStats {
  public static final AttributeKey<Long> FIRST_HANDLER_TIMESTAMP_KEY = AttributeKey.valueOf("FirstHandlerTimestamp");

  private final AtomicInteger activeConnections = new AtomicInteger();

  private final AtomicInteger activeReadHandlerThreads = new AtomicInteger();
  private final Sensor writeAndFlushTimeOkRequests;
  private final Sensor writeAndFlushTimeBadRequests;
  private final Sensor writeAndFlushTimeCombined;
  private final Sensor writeAndFlushCompletionTimeForDataRequest;
  private final Sensor timeSpentInReadHandler;
  private final Sensor timeSpentTillHandoffToReadHandler;
  private final AtomicInteger queuedTasksForReadHandler = new AtomicInteger();

  public VeniceServerNettyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> activeConnections.get(), "active_connections"));

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> activeReadHandlerThreads.get(), "active_read_handler_threads"));

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> queuedTasksForReadHandler.get(), "queued_tasks_for_read_handler"));

    String writeAndFlushTimeCombinedSensorName = "WriteAndFlushTimeCombined";
    writeAndFlushTimeCombined = registerSensorIfAbsent(
        writeAndFlushTimeCombinedSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + writeAndFlushTimeCombinedSensorName));

    String writeAndFlushTimeOkRequestsSensorName = "WriteAndFlushTimeOkRequests";
    writeAndFlushTimeOkRequests = registerSensorIfAbsent(
        writeAndFlushTimeOkRequestsSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + writeAndFlushTimeOkRequestsSensorName));

    String writeAndFlushTimeBadRequestsSensorName = "WriteAndFlushTimeBadRequests";
    writeAndFlushTimeBadRequests = registerSensorIfAbsent(
        writeAndFlushTimeBadRequestsSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + writeAndFlushTimeBadRequestsSensorName));

    String responseWriteAndFlushStartTimeNanosSensorName = "WriteAndFlushCompletionTimeForDataRequest";
    writeAndFlushCompletionTimeForDataRequest = registerSensorIfAbsent(
        responseWriteAndFlushStartTimeNanosSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + responseWriteAndFlushStartTimeNanosSensorName));

    String timeSpentInReadHandlerSensorName = "TimeSpentInReadHandler";
    timeSpentInReadHandler = registerSensorIfAbsent(
        timeSpentInReadHandlerSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + timeSpentInReadHandlerSensorName));

    String timeSpentTillHandoffToReadHandlerSensorName = "TimeSpentTillHandoffToReadHandler";
    timeSpentTillHandoffToReadHandler = registerSensorIfAbsent(
        timeSpentTillHandoffToReadHandlerSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + timeSpentTillHandoffToReadHandlerSensorName));
  }

  public static long getElapsedTimeInMicros(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos) / 1000;
  }

  public static long getElapsedTimeInNanos(long startTimeNanos) {
    return System.nanoTime() - startTimeNanos;
  }

  public int incrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.incrementAndGet();
  }

  public int decrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.decrementAndGet();
  }

  public int incrementActiveConnections() {
    return activeConnections.incrementAndGet();
  }

  public int decrementActiveConnections() {
    return activeConnections.decrementAndGet();
  }

  public void recordWriteAndFlushTimeOkRequests(long startTimeNanos) {
    writeAndFlushTimeOkRequests.record(getElapsedTimeInMicros(startTimeNanos));
    writeAndFlushTimeCombined.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void recordWriteAndFlushTimeBadRequests(long startTimeNanos) {
    writeAndFlushTimeBadRequests.record(getElapsedTimeInMicros(startTimeNanos));
    writeAndFlushTimeCombined.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void recordWriteAndFlushCompletionTimeForDataRequest(long startTimeNanos) {
    writeAndFlushCompletionTimeForDataRequest.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void incrementQueuedTasksForReadHandler() {
    queuedTasksForReadHandler.incrementAndGet();
  }

  public void decrementQueuedTasksForReadHandler() {
    queuedTasksForReadHandler.decrementAndGet();
  }

  public void recordTimeSpentInReadHandler(long startTimeNanos) {
    timeSpentInReadHandler.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void recordTimeSpentTillHandoffToReadHandler(long startTimeNanos) {
    timeSpentTillHandoffToReadHandler.record(getElapsedTimeInMicros(startTimeNanos));
  }
}
