package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;


public class ParticipantStoreConsumptionStats extends AbstractVeniceStats {
  private static final String NAME_SUFFIX = "-participant_store_consumption_task";
  /**
   * Latency in ms for when the kill signal was generated in the child controller to when the kill is performed in the
   * storage node.
   */
  private static final String KILL_PUSH_JOB_LATENCY = "kill_push_job_latency";
  /**
   * Counter for the number of push job that is schedule to be killed on the storage node.
   */
  private static final String KILLED_PUSH_JOBS = "killed_push_jobs";
  /**
   * Counter for the number of times that the participant store consumption task failed to start.
   */
  private static final String FAILED_INITIALIZATION = "failed_initialization";
  /**
   * Counter for the number of exceptions thrown during the consumption of the participant store, specifically for
   * {@link com.linkedin.venice.participant.protocol.KillPushJob} records.
   */
  private static final String KILL_PUSH_JOB_FAILED_CONSUMPTION = "kill_push_job_failed_consumption";

  private static final String HEARTBEAT = "heartbeat";

  private final Sensor killPushJobLatencySensor;
  private final Sensor killedPushJobsSensor;
  private final Sensor failedInitializationSensor;
  private final Sensor killPushJobFailedConsumption;
  private final Sensor heartbeatSensor;

  public ParticipantStoreConsumptionStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName + NAME_SUFFIX);
    killPushJobLatencySensor = registerSensorIfAbsent(KILL_PUSH_JOB_LATENCY, new Avg(), new Max());
    killedPushJobsSensor = registerSensorIfAbsent(KILLED_PUSH_JOBS, new Count());
    failedInitializationSensor = registerSensorIfAbsent(FAILED_INITIALIZATION, new Count());
    killPushJobFailedConsumption = registerSensorIfAbsent(KILL_PUSH_JOB_FAILED_CONSUMPTION, new Count());
    heartbeatSensor = registerSensorIfAbsent(HEARTBEAT, new OccurrenceRate());
  }

  public void recordKillPushJobLatency(double latencyInMs) {
    killPushJobLatencySensor.record(latencyInMs);
  }

  public void recordKilledPushJobs() {
    killedPushJobsSensor.record();
  }

  public void recordFailedInitialization() {
    failedInitializationSensor.record();
  }

  public void recordKillPushJobFailedConsumption() {
    killPushJobFailedConsumption.record();
  }

  public void recordHeartbeat() {
    heartbeatSensor.record();
  }
}
