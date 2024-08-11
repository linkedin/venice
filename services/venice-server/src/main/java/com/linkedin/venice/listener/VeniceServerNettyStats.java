package com.linkedin.venice.listener;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.EventLoopGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.concurrent.atomic.AtomicInteger;


public class VeniceServerNettyStats extends AbstractVeniceStats {
  public static final AttributeKey<Long> FIRST_HANDLER_TIMESTAMP_KEY = AttributeKey.valueOf("FirstHandlerTimestamp");

  private final AtomicInteger activeReadHandlerThreads = new AtomicInteger();
  // queued_tasks_for_read_handler
  private final AtomicInteger queuedTasksForReadHandler = new AtomicInteger();

  private final AtomicInteger allInflightRequests = new AtomicInteger();
  private final AtomicInteger ioInflightRequests = new AtomicInteger();

  private final Sensor timeSpentTillHandoffToReadHandler;
  private final Sensor timeSpentInQuotaEnforcement;
  private final Sensor nettyFlushCounter;

  private final Sensor storageExecutionHandlerSubmissionWaitTime;
  private final Sensor nonOkResponseLatency;
  private final Sensor requestArrivalRate;
  private final Sensor requestProcessingRate;

  private final Sensor ioRequestArrivalRate;
  private final Sensor ioRequestProcessingRate;
  private final Sensor multiGetStorageLayerProcessingRate;

  private final Sensor allInflightRequestsSensorGauge;
  private final Sensor allInflightRequestsSensor;
  private final Sensor ioInflightRequestsSensorGauge;
  private final Sensor ioInflightRequestsSensor;

  private final Sensor nettyIoThreadsPendingTasks;
  private final Sensor writeAndFlushTimeSensor;

  private EventLoopGroup eventLoopGroup;
  private Sensor channelNotWritable;

  PriorityBasedResponseScheduler priorityBasedResponseScheduler;
  // private final Sensor getTimeSpentTillHandoffToReadHandler;

  // NettWriteTask.PriorityBasedThreadPoolExecutor priorityBasedThreadPoolExecutor =
  // new NettWriteTask.PriorityBasedThreadPoolExecutor(4);

  public void setPriorityBasedResponseScheduler(PriorityBasedResponseScheduler priorityBasedResponseScheduler) {
    this.priorityBasedResponseScheduler = priorityBasedResponseScheduler;
  }

  public PriorityBasedResponseScheduler getPriorityBasedResponseScheduler() {
    return priorityBasedResponseScheduler;
  }

  // public NettWriteTask.PriorityBasedThreadPoolExecutor getPriorityBasedThreadPoolExecutor() {
  // return priorityBasedThreadPoolExecutor;
  // }

  public VeniceServerNettyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    nettyIoThreadsPendingTasks = registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> getPendingNettyIoTasks(), "netty_io_threads_pending_tasks"));
    allInflightRequestsSensorGauge = registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> allInflightRequests.get(), "all_inflight_requests_gauge"));
    allInflightRequestsSensor = registerSensorIfAbsent("all_inflight_requests", new Avg(), new Max());

    ioInflightRequestsSensorGauge = registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> ioInflightRequests.get(), "io_inflight_requests_gauge"));

    ioInflightRequestsSensor = registerSensorIfAbsent("io_inflight_requests", new Avg(), new Max());

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> activeReadHandlerThreads.get(), "active_read_handler_threads"));
    // queued_tasks_for_read_handler
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> queuedTasksForReadHandler.get(), "queued_tasks_for_read_handler"));

    nettyFlushCounter = registerSensor("nettyFlushCounter", new Rate(), new Avg(), new Max());

    channelNotWritable = registerSensorIfAbsent("channel_not_writable", new OccurrenceRate());

    String timeSpentTillHandoffToReadHandlerSensorName = "TimeSpentTillHandoffToReadHandler";
    timeSpentTillHandoffToReadHandler = registerSensorIfAbsent(
        timeSpentTillHandoffToReadHandlerSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + timeSpentTillHandoffToReadHandlerSensorName));

    String timeSpentInQuotaEnforcementSensorName = "TimeSpentInQuotaEnforcement";
    timeSpentInQuotaEnforcement = registerSensorIfAbsent(
        timeSpentInQuotaEnforcementSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + timeSpentInQuotaEnforcementSensorName));

    String storageExecutionHandlerSubmissionWaitTimeSensorName = "storage_execution_handler_submission_wait_time";

    storageExecutionHandlerSubmissionWaitTime = registerSensorIfAbsent(
        storageExecutionHandlerSubmissionWaitTimeSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + storageExecutionHandlerSubmissionWaitTimeSensorName));

    String nonOkResponseLatencySensorName = "non_ok_response_latency";
    nonOkResponseLatency = registerSensorIfAbsent(
        nonOkResponseLatencySensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + nonOkResponseLatencySensorName));

    String requestArrivalRateSensorName = "request_arrival_rate";

    requestArrivalRate = registerSensorIfAbsent(requestArrivalRateSensorName, new OccurrenceRate());

    String requestProcessingRateSensorName = "request_processing_rate";
    requestProcessingRate = registerSensorIfAbsent(requestProcessingRateSensorName, new OccurrenceRate());

    String ioRequestArrivalRateSensorName = "io_request_arrival_rate";
    ioRequestArrivalRate = registerSensorIfAbsent(ioRequestArrivalRateSensorName, new OccurrenceRate());

    String ioRequestProcessingRateSensorName = "io_request_processing_rate";
    ioRequestProcessingRate = registerSensorIfAbsent(
        ioRequestProcessingRateSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + ioRequestProcessingRateSensorName));

    String multiGetStorageLayerProcessingRateSensorName = "multiget_storage_layer_processing_rate";
    multiGetStorageLayerProcessingRate = registerSensorIfAbsent(
        multiGetStorageLayerProcessingRateSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + multiGetStorageLayerProcessingRateSensorName));

    String writeAndFlushTimeSensorName = "write_and_flush_time";
    writeAndFlushTimeSensor = registerSensorIfAbsent(
        writeAndFlushTimeSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + writeAndFlushTimeSensorName));
  }

  private static final double NANO_TO_MILLIS = 1_000_000;

  public static double getElapsedTimeInMillis(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos) / NANO_TO_MILLIS;
  }

  public int incrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.incrementAndGet();
  }

  public int decrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.decrementAndGet();
  }

  // queued_tasks_for_read_handler
  public int incrementQueuedTasksForReadHandler() {
    return queuedTasksForReadHandler.incrementAndGet();
  }

  // queued_tasks_for_read_handler
  public int decrementQueuedTasksForReadHandler() {
    return queuedTasksForReadHandler.decrementAndGet();
  }

  public void recordTimeSpentTillHandoffToReadHandler(long startTimeNanos) {
    timeSpentTillHandoffToReadHandler.record(getElapsedTimeInMillis(startTimeNanos));
  }

  public void recordTimeSpentInQuotaEnforcement(long startTimeNanos) {
    timeSpentInQuotaEnforcement.record(getElapsedTimeInMillis(startTimeNanos));
  }

  public void recordNettyFlushCounts() {
    nettyFlushCounter.record(1);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    storageExecutionHandlerSubmissionWaitTime.record(submissionWaitTime);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(long startTimeNanos, long endTimeNanos) {
    storageExecutionHandlerSubmissionWaitTime.record(LatencyUtils.convertNSToMS(endTimeNanos - startTimeNanos));
  }

  public void recordNonOkResponseLatency(double latency) {
    nonOkResponseLatency.record(latency);
  }

  public void recordRequestArrivalRate() {
    requestArrivalRate.record();
  }

  public void recordRequestProcessingRate() {
    requestProcessingRate.record();
  }

  public void recordIoRequestArrivalRate() {
    ioRequestArrivalRate.record();
  }

  public void recordIoRequestProcessingRate(double elapsedTime) {
    ioRequestProcessingRate.record(elapsedTime);
  }

  public void recordMultiGetStorageLayerProcessingRate(double elapsedTime) {
    multiGetStorageLayerProcessingRate.record(elapsedTime);
  }

  public void incrementAllInflightRequests() {
    allInflightRequestsSensor.record(allInflightRequests.incrementAndGet());
  }

  public void decrementAllInflightRequests() {
    allInflightRequestsSensor.record(allInflightRequests.decrementAndGet());
  }

  public void incrementIoInflightRequests() {
    ioInflightRequestsSensor.record(ioInflightRequests.incrementAndGet());
  }

  public void decrementIoInflightRequests() {
    ioInflightRequestsSensor.record(ioInflightRequests.decrementAndGet());
  }

  public void recordWriteAndFlushTime(long startTimeNanos) {
    writeAndFlushTimeSensor.record(getElapsedTimeInMillis(startTimeNanos));
  }

  public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
    this.eventLoopGroup = eventLoopGroup;
  }

  public long getPendingNettyIoTasks() {
    if (eventLoopGroup == null) {
      return -1;
    }

    long totalPendingTasks = 0;
    for (final EventExecutor eventExecutor: eventLoopGroup) {
      if (eventExecutor instanceof SingleThreadEventExecutor) {
        int pendingTasks = ((SingleThreadEventExecutor) eventExecutor).pendingTasks();
        totalPendingTasks += pendingTasks;
      }
    }
    return totalPendingTasks;
  }

  public void recordChannelNotWritable() {
    channelNotWritable.record();
  }
}
