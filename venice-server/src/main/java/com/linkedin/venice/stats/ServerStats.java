package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import org.apache.log4j.Logger;

import javax.validation.constraints.NotNull;
import java.util.List;

public class ServerStats extends AbstractVeniceStats {
  private final static Logger logger = Logger.getLogger(ServerStats.class);
  private final KafkaConsumerPerStoreService kafkaConsumerPerStoreService;

  //Sensors for measuring BnP job
  private final Sensor bytesConsumedSensor;
  private final Sensor recordsConsumedSensor;

  //Sensors for measuring requests from Router
  //request for data
  private final Sensor successRequestSensor;
  private final Sensor errorRequestSensor;
  private final Sensor successRequestLatencySensor;
  private final Sensor errorRequestLatencySensor;
  private final Sensor successRequestRatioSensor;
  private final Sensor kafkaOffsetLagSensor;
  private final Sensor bdbQueryLatencySensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor processPollResultLatencySensor;
  private final Sensor pollRequestSensor;
  private final Sensor pollResultNumSensor;


  public ServerStats(@NotNull MetricsRepository metricsRepository,
                     @NotNull String name,
                     @NotNull KafkaConsumerPerStoreService kafkaConsumerPerStoreService) {
    super(metricsRepository, name);
    this.kafkaConsumerPerStoreService = kafkaConsumerPerStoreService;

    bytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    recordsConsumedSensor = registerSensor("records_consumed", new Rate());

    MeasurableStat successRequest = new Count();
    MeasurableStat errorRequest = new Count();
    successRequestSensor = registerSensor("success_request", successRequest, new OccurrenceRate());
    errorRequestSensor = registerSensor("error_request", errorRequest, new OccurrenceRate());
    successRequestLatencySensor = registerSensor("success_request_latency",
      TehutiUtils.getPercentileStat(getName() + "_" + "success_request_latency"));
    errorRequestLatencySensor = registerSensor("error_request_latency",
      TehutiUtils.getPercentileStat(getName() + "_" + "error_request_latency"));
    successRequestRatioSensor = registerSensor("success_request_ratio",
      TehutiUtils.getRatioStat(successRequest, errorRequest));

    //bdbQueryLatency is normally less than 1 ms. Record ns instead of ms for better readability.
    bdbQueryLatencySensor = registerSensor("bdb_query_latency_ns",
      TehutiUtils.getPercentileStat(getName() + "_" + "bdb_query_latency"));

    //KafkaOffsetLag is the lag of the most recent offsets between local kafka and storage node.
    //Right now, it emits the max lag in every sliding window.
    //If a storage node has multiple partitions for a store, they will be aggregated.
    kafkaOffsetLagSensor = registerSensor("kafka_offset_lag",
      new OffsetLagStat(kafkaConsumerPerStoreService, getName().substring(1)));

    // Measure latency of Kafka consumer poll request and processing returned consumer records
    pollRequestSensor = registerSensor("kafka_poll_request", new Count());
    pollRequestLatencySensor = registerSensor("kafka_poll_request_latency", new Avg());
    processPollResultLatencySensor = registerSensor("process_poll_result_latency", new Avg());
    // TODO: need to convert 'Total' to be 'Count' internally so that we can measure
    // consumer record number per second returned by Kafka consumer poll.
    pollResultNumSensor = registerSensor("kafka_poll_result_num", new Avg(), new Total());
  }

  public void recordBytesConsumed(long bytes) {
    bytesConsumedSensor.record(bytes);
  }

  public void recordRecordsConsumed(int count) {
    recordsConsumedSensor.record(count);
  }

  public void recordSuccessRequest() {
    record(successRequestSensor);
  }

  public void recordErrorRequest() {
    record(errorRequestSensor);
  }

  public void recordSuccessRequestLatency(double latency) {
    record(successRequestLatencySensor, latency);
  }

  public void recordErrorRequestLatency(double latency) {
    record(errorRequestLatencySensor, latency);
  }

  public void recordBdbQueryLatency(double latency) {
    record(bdbQueryLatencySensor, latency);
  }

  public void recordPollRequest() {
    record(pollRequestSensor);
  }

  public void recordPollRequestLatency(double latency) {
    pollRequestLatencySensor.record(latency);
  }

  public void recordProcessPollResultLatency(double latency) {
    processPollResultLatencySensor.record(latency);
  }

  public void recordPollResultNum(int count) {
    pollResultNumSensor.record(count);
  }
}
