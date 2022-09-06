package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.SampledTotal;
import java.util.Optional;


/**
 * This class is used to monitor the various reasons for unhealthy hosts.
 */
public class HostHealthStats extends AbstractVeniceStats {
  private final Sensor unhealthyHostOfflineInstance;
  private final Sensor unhealthyHostTooManyPendingRequest;
  private final Sensor unhealthyHostHeartBeatFailure;
  private final Sensor pendingRequestCount;
  private final Sensor leakedPendingRequestCount;
  private final Sensor unhealthyPendingQueueDuration;
  private final Sensor unhealthyPendingRateSensor;
  private final Sensor unhealthyHostDelayJoinSensor;
  private Optional<Sensor> unhealthyHostCountCausedByPendingQueueSensor = Optional.empty();
  private Optional<Sensor> unhealthyHostCountCausedByHeartBeatSensor = Optional.empty();

  public HostHealthStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    this.unhealthyHostOfflineInstance = registerSensor("unhealthy_host_offline_instance", new Count());
    this.unhealthyHostTooManyPendingRequest = registerSensor("unhealthy_host_too_many_pending_request", new Count());
    this.unhealthyHostHeartBeatFailure = registerSensor("unhealthy_host_heart_beat_failure", new Count());
    this.pendingRequestCount = registerSensor("pending_request_count", new Max());
    this.leakedPendingRequestCount = registerSensor("leaked_pending_request_count", new Count());
    this.unhealthyPendingQueueDuration =
        registerSensor("unhealthy_pending_queue_duration", new Avg(), new Min(), new Max(), new SampledTotal());
    ;
    this.unhealthyPendingRateSensor = registerSensor("unhealthy_pending_queue", new OccurrenceRate());
    this.unhealthyHostDelayJoinSensor = registerSensor("unhealthy_host_delay_join", new OccurrenceRate());
    if (name.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
      // This is trying to avoid emit unnecessary metrics per route
      this.unhealthyHostCountCausedByPendingQueueSensor =
          Optional.of(registerSensor("unhealthy_host_count_caused_by_pending_queue", new Avg(), new Min(), new Max()));
      this.unhealthyHostCountCausedByHeartBeatSensor = Optional
          .of(registerSensor("unhealthy_host_count_caused_by_router_heart_beat", new Avg(), new Min(), new Max()));
    }
  }

  public void recordUnhealthyHostOfflineInstance() {
    unhealthyHostOfflineInstance.record();
  }

  public void recordUnhealthyHostTooManyPendingRequest() {
    unhealthyHostTooManyPendingRequest.record();
  }

  public void recordUnhealthyHostHeartBeatFailure() {
    unhealthyHostHeartBeatFailure.record();
  }

  public void recordPendingRequestCount(long cnt) {
    pendingRequestCount.record(cnt);
  }

  public void recordLeakedPendingRequestCount() {
    leakedPendingRequestCount.record();
  }

  public void recordUnhealthyPendingQueueDuration(double duration) {
    unhealthyPendingRateSensor.record();
    unhealthyPendingQueueDuration.record(duration);
  }

  public void recordUnhealthyHostCountCausedByPendingQueue(int count) {
    unhealthyHostCountCausedByPendingQueueSensor.ifPresent(sensor -> sensor.record(count));
  }

  public void recordUnhealthyHostCountCausedByRouterHeartBeat(int count) {
    unhealthyHostCountCausedByHeartBeatSensor.ifPresent(sensor -> sensor.record(count));
  }

  public void recordUnhealthyHostDelayJoin() {
    unhealthyHostDelayJoinSensor.record();
  }
}
