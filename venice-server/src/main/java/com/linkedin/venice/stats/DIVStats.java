package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;


public class DIVStats {
  private long duplicateMsg = 0;
  private long missingMsg = 0;
  private long corruptedMsg = 0;
  private long successMsg = 0;
  private long currentIdleTime = 0;
  private long overallIdleTime = 0;

  private final MetricConfig metricConfig = new MetricConfig();
  /**
   * Creating this separate local MetricsRepository only to utilize the Sensor library and not for reporting.
   * Reporting is done at the higher level by the DIVStatsReporter.
   */
  private final MetricsRepository localRepository = new MetricsRepository(metricConfig);
  private final Sensor producerBrokerLatencySensor;
  private final Sensor brokerConsumerLatencySensor;
  private final Sensor producerConsumerLatencySensor;

  private Avg producerBrokerLatencyAvgMs = new Avg();
  private Min producerBrokerLatencyMinMs = new Min();
  private Max producerBrokerLatencyMaxMs = new Max();
  private Avg brokerConsumerLatencyAvgMs = new Avg();
  private Min brokerConsumerLatencyMinMs = new Min();
  private Max brokerConsumerLatencyMaxMs = new Max();
  private Avg producerConsumerLatencyAvgMs = new Avg();
  private Min producerConsumerLatencyMinMs = new Min();
  private Max producerConsumerLatencyMaxMs = new Max();

  public DIVStats() {
    String sensorName = "producer_to_broker_latency";
    producerBrokerLatencySensor = localRepository.sensor(sensorName);
    producerBrokerLatencySensor.add(sensorName+ producerBrokerLatencyAvgMs.getClass().getSimpleName(),
        producerBrokerLatencyAvgMs);
    producerBrokerLatencySensor.add(sensorName + producerBrokerLatencyMaxMs.getClass().getSimpleName(),
        producerBrokerLatencyMaxMs);
    producerBrokerLatencySensor.add(sensorName + producerBrokerLatencyMinMs.getClass().getSimpleName(),
        producerBrokerLatencyMinMs);

    sensorName = "broker_to_consumer_latency";
    brokerConsumerLatencySensor = localRepository.sensor(sensorName);
    brokerConsumerLatencySensor.add(sensorName + brokerConsumerLatencyAvgMs.getClass().getSimpleName(),
        brokerConsumerLatencyAvgMs);
    brokerConsumerLatencySensor.add(sensorName + brokerConsumerLatencyMaxMs.getClass().getSimpleName(),
        brokerConsumerLatencyMaxMs);
    brokerConsumerLatencySensor.add(sensorName + brokerConsumerLatencyMinMs.getClass().getSimpleName(),
        brokerConsumerLatencyMinMs);

    sensorName = "producer_to_consumer_latency";
    producerConsumerLatencySensor = localRepository.sensor(sensorName);
    producerConsumerLatencySensor.add(sensorName + producerConsumerLatencyAvgMs.getClass().getSimpleName(),
        producerConsumerLatencyAvgMs);
    producerConsumerLatencySensor.add(sensorName + producerConsumerLatencyMaxMs.getClass().getSimpleName(),
        producerConsumerLatencyMaxMs);
    producerConsumerLatencySensor.add(sensorName + producerConsumerLatencyMinMs.getClass().getSimpleName(),
        producerConsumerLatencyMinMs);
  }

  public long getDuplicateMsg() {
    return duplicateMsg;
  }

  public void recordDuplicateMsg() {
    duplicateMsg += 1;
  }

  public long getMissingMsg() {
    return missingMsg;
  }

  public void recordMissingMsg() {
    missingMsg += 1;
  }

  public long getCorruptedMsg() {
    return corruptedMsg;
  }

  public void recordCorruptedMsg() {
    corruptedMsg += 1;
  }

  public long getSuccessMsg() {
    return successMsg;
  }

  public void recordSuccessMsg() {
    successMsg += 1;
  }

  public long getCurrentIdleTime() {
    return currentIdleTime;
  }

  public void recordCurrentIdleTime() {
    currentIdleTime += 1;
  }

  public void resetCurrentIdleTime() {
    currentIdleTime = 0;
  }

  public long getOverallIdleTime() {
    return overallIdleTime;
  }

  public void recordOverallIdleTime() {
    overallIdleTime += 1;
  }

  public double getProducerBrokerLatencyAvgMs() {
    return producerBrokerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerBrokerLatencyMinMs() {
    return producerBrokerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerBrokerLatencyMaxMs() {
    return producerBrokerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerBrokerLatencyMs(double value) {
    producerBrokerLatencySensor.record(value);
  }

  public double getBrokerConsumerLatencyAvgMs() {
    return brokerConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getBrokerConsumerLatencyMinMs() {
    return brokerConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getBrokerConsumerLatencyMaxMs() {
    return brokerConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordBrokerConsumerLatencyMs(double value) {
    brokerConsumerLatencySensor.record(value);
  }

  public double getProducerConsumerLatencyAvgMs() {
    return producerConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerConsumerLatencyMinMs() {
    return producerConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerConsumerLatencyMaxMs() {
    return producerConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerConsumerLatencyMs(double value) {
    producerConsumerLatencySensor.record(value);
  }
}
