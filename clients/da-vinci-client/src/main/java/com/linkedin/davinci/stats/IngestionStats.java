package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.utils.RegionUtils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Rate;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;


/**
 * This class contains stats for store ingestion. The stat class is used in {@link VeniceVersionedStats} to serve for
 * a single store version or total of all store versions.
 * This class does not contain reporting logic as reporting is done by the {@link IngestionStatsReporter}.
 */
public class IngestionStats {
  protected static final String INGESTION_TASK_ERROR_GAUGE = "ingestion_task_errored_gauge";
  protected static final String INGESTION_TASK_PUSH_TIMEOUT_GAUGE = "ingestion_task_push_timeout_gauge";
  protected static final String WRITE_COMPUTE_OPERATION_FAILURE = "write_compute_operation_failure";
  protected static final String RECORDS_CONSUMED_METRIC_NAME = "records_consumed";
  protected static final String BYTES_CONSUMED_METRIC_NAME = "bytes_consumed";
  protected static final String LEADER_RECORDS_CONSUMED_METRIC_NAME = "leader_records_consumed";
  protected static final String LEADER_BYTES_CONSUMED_METRIC_NAME = "leader_bytes_consumed";
  protected static final String FOLLOWER_RECORDS_CONSUMED_METRIC_NAME = "follower_records_consumed";
  protected static final String FOLLOWER_BYTES_CONSUMED_METRIC_NAME = "follower_bytes_consumed";
  protected static final String LEADER_RECORDS_PRODUCED_METRIC_NAME = "leader_records_produced";
  protected static final String LEADER_BYTES_PRODUCED_METRIC_NAME = "leader_bytes_produced";
  protected static final String SUBSCRIBE_ACTION_PREP_LATENCY = "subscribe_action_prep_latency";
  protected static final String UPDATE_IGNORED_DCR = "update_ignored_dcr";
  protected static final String TOTAL_DCR = "total_dcr";
  protected static final String TOTAL_DUPLICATE_KEY_UPDATE_COUNT = "total_duplicate_key_update_count";
  protected static final String TIMESTAMP_REGRESSION_DCR_ERROR = "timestamp_regression_dcr_error";
  protected static final String OFFSET_REGRESSION_DCR_ERROR = "offset_regression_dcr_error";
  protected static final String TOMBSTONE_CREATION_DCR = "tombstone_creation_dcr";

  /**
   * Consumer metric: Measures the total time from when a record starts being processed (after polling from Kafka and
   * schema checks) until it completes all processing stages. This includes leader preprocessing, producing to local
   * Kafka (for leaders), queueing to drainer, drainer processing (persisting to storage), and offset updates.
   */
  protected static final String CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY =
      "consumed_record_end_to_end_processing_latency";
  /**
   * Leader metric: Measures the latency from when a nearline producer originally produced a message (with its producer
   * timestamp) to when that message is successfully written to the local broker's version topic by the leader.
   */
  public static final String NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY = "nearline_producer_to_local_broker_latency";

  /**
   * Leader metric: Measures the latency from when a producer created a message (producer timestamp) to when the
   * source broker (remote region Kafka) received it.
   */
  public static final String PRODUCER_TO_SOURCE_BROKER_LATENCY = "producer_to_source_broker_latency";

  /**
   * Leader metric: Measures the latency from when the source broker (remote region Kafka) received a message to when
   * the leader consumer fetched it.
   */
  public static final String SOURCE_BROKER_TO_LEADER_CONSUMER_LATENCY = "source_broker_to_leader_consumer_latency";

  /**
   * Follower metric: Measures the latency from when the leader produced a message to when the local broker
   * (local region Kafka) received it.
   */
  public static final String PRODUCER_TO_LOCAL_BROKER_LATENCY = "producer_to_local_broker_latency";

  /**
   * Follower metric: Measures the latency from when the local broker (local region Kafka) received a message to when
   * the follower consumer fetched it.
   */
  public static final String LOCAL_BROKER_TO_FOLLOWER_CONSUMER_LATENCY = "local_broker_to_follower_consumer_latency";

  /**
   * Leader metric: Measures the time from when a produce call is made to when the producer callback is invoked,
   * indicating how long Kafka took to write the message to the broker and invoke the callback.
   */
  public static final String LEADER_PRODUCER_COMPLETION_LATENCY = "leader_producer_completion_latency";

  public static final String IDLE_TIME = "idle_time";
  /**
   * Leader metric: Measures the time spent within the leader's producer callback processing after a message is
   * successfully produced to the local broker. This includes chunking processing, producing to drainer buffer service,
   * producing deprecated chunk deletions, and recording stats.
   */
  public static final String PRODUCER_CALLBACK_LATENCY = "producer_callback_latency";
  /**
   * Leader metric: Measures the time from when keys are locked for a batch of records to just before producing to Kafka.
   * When batch processing is enabled: includes batch processing (with partial update operations), record validation,
   * and for real-time topics, recording hybrid consumption stats.
   * When batch processing is disabled: includes only record validation and stats recording; partial update happens later.
   */
  public static final String LEADER_PREPROCESSING_LATENCY = "leader_preprocessing_latency";
  /**
   * Drainer metric: Measures the time spent on lightweight preprocessing tasks before heavy message processing begins.
   * This includes recording write path latency stats (producer-to-broker, broker-to-consumer), drainer message
   * validation, and reporting batch end of incremental push status. Recorded at the start of internalProcessConsumerRecord,
   * before control message or data message processing.
   */
  public static final String INTERNAL_PREPROCESSING_LATENCY = "internal_preprocessing_latency";

  public static final String BATCH_PROCESSING_REQUEST = "batch_processing_request";
  public static final String BATCH_PROCESSING_REQUEST_SIZE = "batch_processing_request_size";
  public static final String BATCH_PROCESSING_REQUEST_RECORDS = "batch_processing_request_records";
  public static final String BATCH_PROCESSING_REQUEST_LATENCY = "batch_processing_request_latency";
  public static final String BATCH_PROCESSING_REQUEST_ERROR = "batch_processing_request_error";

  public static final String STORAGE_QUOTA_USED = "storage_quota_used";

  private static final MetricConfig METRIC_CONFIG = new MetricConfig();
  private StoreIngestionTask ingestionTask;
  private int ingestionTaskPushTimeoutGauge = 0;
  private final Int2ObjectMap<Rate> regionIdToHybridBytesConsumedRateMap;
  private final Int2ObjectMap<Rate> regionIdToHybridRecordsConsumedRateMap;
  private final LongAdderRateGauge recordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge bytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderRecordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderBytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge followerRecordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge followerBytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderRecordsProducedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderBytesProducedSensor = new LongAdderRateGauge();
  private final Int2ObjectMap<Sensor> regionIdToHybridBytesConsumedSensorMap;
  private final Int2ObjectMap<Sensor> regionIdToHybridRecordsConsumedSensorMap;

  // write path latency sensors
  private final WritePathLatencySensor producerSourceBrokerLatencySensor;
  private final WritePathLatencySensor sourceBrokerLeaderConsumerLatencySensor;
  private final WritePathLatencySensor producerLocalBrokerLatencySensor;
  private final WritePathLatencySensor localBrokerFollowerConsumerLatencySensor;
  private final WritePathLatencySensor leaderProducerCompletionLatencySensor;
  private final WritePathLatencySensor subscribePrepLatencySensor;
  private final WritePathLatencySensor consumedRecordEndToEndProcessingLatencySensor;
  private final WritePathLatencySensor nearlineProducerToLocalBrokerLatencySensor;
  private final WritePathLatencySensor producerCallBackLatency;
  private final WritePathLatencySensor leaderPreprocessingLatency;
  private final WritePathLatencySensor internalPreprocessingLatency;
  // Measure the count of ignored updates due to conflict resolution
  private final LongAdderRateGauge conflictResolutionUpdateIgnoredSensor = new LongAdderRateGauge();
  // Measure the total number of incoming conflict resolutions
  private final LongAdderRateGauge totalConflictResolutionCountSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge timestampRegressionDCRErrorSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge offsetRegressionDCRErrorSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge tombstoneCreationDCRSensor = new LongAdderRateGauge();

  private final Count totalDuplicateKeyUpdateCount = new Count();
  private final Sensor totalDuplicateKeyUpdateCountSensor;

  private final MetricsRepository localMetricRepository;

  // Measure the max idle time among partitions for a given the store on this host
  private final LongAdderRateGauge idleTimeSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge batchProcessingRequestSensor = new LongAdderRateGauge();
  private final WritePathLatencySensor batchProcessingRequestSizeSensor;
  private final LongAdderRateGauge batchProcessingRequestRecordsSensor = new LongAdderRateGauge();
  private final WritePathLatencySensor batchProcessingRequestLatencySensor;
  private final LongAdderRateGauge batchProcessingRequestErrorSensor = new LongAdderRateGauge();

  public IngestionStats(VeniceServerConfig serverConfig) {

    Int2ObjectMap<String> kafkaClusterIdToAliasMap = serverConfig.getKafkaClusterIdToAliasMap();
    regionIdToHybridBytesConsumedRateMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridBytesConsumedSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridRecordsConsumedRateMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridRecordsConsumedSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());

    localMetricRepository = new MetricsRepository(METRIC_CONFIG);
    for (Int2ObjectMap.Entry<String> entry: kafkaClusterIdToAliasMap.int2ObjectEntrySet()) {
      int regionId = entry.getIntKey();
      String regionNamePrefix =
          RegionUtils.getRegionSpecificMetricPrefix(serverConfig.getRegionName(), entry.getValue());
      Rate regionHybridBytesConsumedRate = new Rate();
      String regionHybridBytesConsumedMetricName = regionNamePrefix + "_rt_bytes_consumed";
      Sensor regionHybridBytesConsumedSensor = localMetricRepository.sensor(regionHybridBytesConsumedMetricName);
      regionHybridBytesConsumedSensor.add(
          regionHybridBytesConsumedMetricName + regionHybridBytesConsumedRate.getClass().getSimpleName(),
          regionHybridBytesConsumedRate);
      regionIdToHybridBytesConsumedRateMap.put(regionId, regionHybridBytesConsumedRate);
      regionIdToHybridBytesConsumedSensorMap.put(regionId, regionHybridBytesConsumedSensor);

      Rate regionHybridRecordsConsumedRate = new Rate();
      String regionHybridRecordsConsumedMetricName = regionNamePrefix + "_rt_records_consumed";
      Sensor regionHybridRecordsConsumedSensor = localMetricRepository.sensor(regionHybridRecordsConsumedMetricName);
      regionHybridRecordsConsumedSensor.add(
          regionHybridRecordsConsumedMetricName + regionHybridRecordsConsumedRate.getClass().getSimpleName(),
          regionHybridRecordsConsumedRate);
      regionIdToHybridRecordsConsumedRateMap.put(regionId, regionHybridRecordsConsumedRate);
      regionIdToHybridRecordsConsumedSensorMap.put(regionId, regionHybridRecordsConsumedSensor);
    }

    registerSensor(localMetricRepository, RECORDS_CONSUMED_METRIC_NAME, recordsConsumedSensor);
    registerSensor(localMetricRepository, BYTES_CONSUMED_METRIC_NAME, bytesConsumedSensor);
    registerSensor(localMetricRepository, LEADER_RECORDS_CONSUMED_METRIC_NAME, leaderRecordsConsumedSensor);
    registerSensor(localMetricRepository, LEADER_BYTES_CONSUMED_METRIC_NAME, leaderBytesConsumedSensor);
    registerSensor(localMetricRepository, FOLLOWER_BYTES_CONSUMED_METRIC_NAME, followerBytesConsumedSensor);
    registerSensor(localMetricRepository, FOLLOWER_RECORDS_CONSUMED_METRIC_NAME, followerRecordsConsumedSensor);
    registerSensor(localMetricRepository, LEADER_RECORDS_PRODUCED_METRIC_NAME, leaderRecordsProducedSensor);
    registerSensor(localMetricRepository, LEADER_BYTES_PRODUCED_METRIC_NAME, leaderBytesProducedSensor);

    totalDuplicateKeyUpdateCountSensor = localMetricRepository.sensor(TOTAL_DUPLICATE_KEY_UPDATE_COUNT);
    totalDuplicateKeyUpdateCountSensor.add(TOTAL_DUPLICATE_KEY_UPDATE_COUNT, totalDuplicateKeyUpdateCount);

    producerSourceBrokerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, PRODUCER_TO_SOURCE_BROKER_LATENCY);
    sourceBrokerLeaderConsumerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, SOURCE_BROKER_TO_LEADER_CONSUMER_LATENCY);
    producerLocalBrokerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, PRODUCER_TO_LOCAL_BROKER_LATENCY);
    localBrokerFollowerConsumerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, LOCAL_BROKER_TO_FOLLOWER_CONSUMER_LATENCY);
    leaderProducerCompletionLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, LEADER_PRODUCER_COMPLETION_LATENCY);
    subscribePrepLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, SUBSCRIBE_ACTION_PREP_LATENCY);
    consumedRecordEndToEndProcessingLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY);
    nearlineProducerToLocalBrokerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY);
    producerCallBackLatency =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, PRODUCER_CALLBACK_LATENCY);
    leaderPreprocessingLatency =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, LEADER_PREPROCESSING_LATENCY);
    internalPreprocessingLatency =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, INTERNAL_PREPROCESSING_LATENCY);

    registerSensor(localMetricRepository, UPDATE_IGNORED_DCR, conflictResolutionUpdateIgnoredSensor);
    registerSensor(localMetricRepository, TOTAL_DCR, totalConflictResolutionCountSensor);
    registerSensor(localMetricRepository, TIMESTAMP_REGRESSION_DCR_ERROR, timestampRegressionDCRErrorSensor);
    registerSensor(localMetricRepository, OFFSET_REGRESSION_DCR_ERROR, offsetRegressionDCRErrorSensor);
    registerSensor(localMetricRepository, TOMBSTONE_CREATION_DCR, tombstoneCreationDCRSensor);
    registerSensor(localMetricRepository, IDLE_TIME, idleTimeSensor);

    registerSensor(localMetricRepository, BATCH_PROCESSING_REQUEST, batchProcessingRequestSensor);
    registerSensor(localMetricRepository, BATCH_PROCESSING_REQUEST_RECORDS, batchProcessingRequestRecordsSensor);
    registerSensor(localMetricRepository, BATCH_PROCESSING_REQUEST_ERROR, batchProcessingRequestErrorSensor);
    batchProcessingRequestSizeSensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, BATCH_PROCESSING_REQUEST_SIZE);
    batchProcessingRequestLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, BATCH_PROCESSING_REQUEST_LATENCY);
  }

  private void registerSensor(MetricsRepository localMetricRepository, String sensorName, LongAdderRateGauge gauge) {
    Sensor sensor = localMetricRepository.sensor(sensorName);
    sensor.add(sensorName + "_rate", gauge);
  }

  public StoreIngestionTask getIngestionTask() {
    return ingestionTask;
  }

  public void setIngestionTask(StoreIngestionTask ingestionTask) {
    this.ingestionTask = ingestionTask;
  }

  // To prevent this metric being too noisy and align with the PreNotificationCheck of reportError, this metric should
  // only be set if the ingestion task errored after EOP is received for any of the partitions.
  public int getIngestionTaskErroredGauge() {
    return IngestionStatsUtils.getIngestionTaskErroredGauge(ingestionTask);
  }

  public int getWriteComputeErrorCode() {
    return IngestionStatsUtils.getWriteComputeErrorCode(ingestionTask);
  }

  public double getSubscribePrepLatencyAvg() {
    return subscribePrepLatencySensor.getAvg();
  }

  public double getSubscribePrepLatencyMax() {
    return subscribePrepLatencySensor.getMax();
  }

  public void recordSubscribePrepLatency(double value, long currentTimeMs) {
    subscribePrepLatencySensor.record(value, currentTimeMs);
  }

  public double getProducerCallBackLatencyMax() {
    return producerCallBackLatency.getMax();
  }

  public void recordProducerCallBackLatency(double value, long currentTimeMs) {
    producerCallBackLatency.record(value, currentTimeMs);
  }

  public double getLeaderPreprocessingLatencyMax() {
    return leaderPreprocessingLatency.getMax();
  }

  public double getLeaderPreprocessingLatencyAvg() {
    return leaderPreprocessingLatency.getAvg();
  }

  public void recordLeaderPreprocessingLatency(double value, long currentTimeMs) {
    leaderPreprocessingLatency.record(value, currentTimeMs);
  }

  public double getInternalPreprocessingLatencyAvg() {
    return internalPreprocessingLatency.getAvg();
  }

  public double getInternalPreprocessingLatencyMax() {
    return internalPreprocessingLatency.getMax();
  }

  public void recordInternalPreprocessingLatency(double value, long currentTimeMs) {
    internalPreprocessingLatency.record(value, currentTimeMs);
  }

  public double getConsumedRecordEndToEndProcessingLatencyAvg() {
    return consumedRecordEndToEndProcessingLatencySensor.getAvg();
  }

  public double getConsumedRecordEndToEndProcessingLatencyMax() {
    return consumedRecordEndToEndProcessingLatencySensor.getMax();
  }

  public void recordConsumedRecordEndToEndProcessingLatency(double value, long currentTimeMs) {
    consumedRecordEndToEndProcessingLatencySensor.record(value, currentTimeMs);
  }

  public double getRecordsConsumed() {
    return recordsConsumedSensor.getRate();
  }

  public void recordRecordsConsumed() {
    recordsConsumedSensor.record(1);
  }

  public double getBytesConsumed() {
    return bytesConsumedSensor.getRate();
  }

  public void recordBytesConsumed(long value) {
    bytesConsumedSensor.record(value);
  }

  public double getLeaderRecordsConsumed() {
    return leaderRecordsConsumedSensor.getRate();
  }

  public void recordLeaderRecordsConsumed() {
    leaderRecordsConsumedSensor.record();
  }

  public double getLeaderBytesConsumed() {
    return leaderBytesConsumedSensor.getRate();
  }

  public void recordLeaderBytesConsumed(long value) {
    leaderBytesConsumedSensor.record(value);
  }

  public double getFollowerRecordsConsumed() {
    return followerRecordsConsumedSensor.getRate();
  }

  public void recordFollowerRecordsConsumed() {
    followerRecordsConsumedSensor.record();
  }

  public double getFollowerBytesConsumed() {
    return followerBytesConsumedSensor.getRate();
  }

  public void recordFollowerBytesConsumed(long value) {
    followerBytesConsumedSensor.record(value);
  }

  public void recordUpdateIgnoredDCR() {
    conflictResolutionUpdateIgnoredSensor.record();
  }

  public void recordTotalDCR() {
    totalConflictResolutionCountSensor.record();
  }

  public void recordTotalDuplicateKeyUpdate() {
    totalDuplicateKeyUpdateCountSensor.record();
  }

  public void recordTimestampRegressionDCRError() {
    timestampRegressionDCRErrorSensor.record();
  }

  public void recordOffsetRegressionDCRError() {
    offsetRegressionDCRErrorSensor.record();
  }

  public void recordTombStoneCreationDCR() {
    tombstoneCreationDCRSensor.record();
  }

  public double getRegionHybridBytesConsumed(int regionId) {
    Rate rate = regionIdToHybridBytesConsumedRateMap.get(regionId);
    return rate != null ? rate.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
  }

  public void recordRegionHybridBytesConsumed(int regionId, double value, long currentTimeMs) {
    Sensor sensor = regionIdToHybridBytesConsumedSensorMap.get(regionId);
    if (sensor != null) {
      sensor.record(value, currentTimeMs);
    }
  }

  public double getRegionHybridRecordsConsumed(int regionId) {
    Rate rate = regionIdToHybridRecordsConsumedRateMap.get(regionId);
    return rate != null ? rate.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
  }

  public void recordRegionHybridRecordsConsumed(int regionId, double value, long currentTimeMs) {
    Sensor sensor = regionIdToHybridRecordsConsumedSensorMap.get(regionId);
    if (sensor != null) {
      sensor.record(value, currentTimeMs);
    }
  }

  public double getLeaderRecordsProduced() {
    return leaderRecordsProducedSensor.getRate();
  }

  public double getUpdateIgnoredRate() {
    return conflictResolutionUpdateIgnoredSensor.getRate();
  }

  public double getTotalDCRRate() {
    return totalConflictResolutionCountSensor.getRate();
  }

  public double getTotalDuplicateKeyUpdateCount() {
    return totalDuplicateKeyUpdateCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }

  public double getTombstoneCreationDCRRate() {
    return tombstoneCreationDCRSensor.getRate();
  }

  public double getTimestampRegressionDCRRate() {
    return timestampRegressionDCRErrorSensor.getRate();
  }

  public double getOffsetRegressionDCRRate() {
    return offsetRegressionDCRErrorSensor.getRate();
  }

  public void recordLeaderRecordsProduced(long value) {
    leaderRecordsProducedSensor.record(value);
  }

  public double getLeaderBytesProduced() {
    return leaderBytesProducedSensor.getRate();
  }

  public void recordLeaderBytesProduced(long value) {
    leaderBytesProducedSensor.record(value);
  }

  public void setIngestionTaskPushTimeoutGauge(int value) {
    ingestionTaskPushTimeoutGauge = value;
  }

  public int getIngestionTaskPushTimeoutGauge() {
    return ingestionTaskPushTimeoutGauge;
  }

  public double getNearlineProducerToLocalBrokerLatencyAvg() {
    return unAvailableToZero(nearlineProducerToLocalBrokerLatencySensor.getAvg());
  }

  public double getNearlineProducerToLocalBrokerLatencyMax() {
    return unAvailableToZero(nearlineProducerToLocalBrokerLatencySensor.getMax());
  }

  public void recordNearlineProducerToLocalBrokerLatency(double value, long currentTimeMs) {
    nearlineProducerToLocalBrokerLatencySensor.record(value, currentTimeMs);
  }

  public void recordIdleTime(long value) {
    idleTimeSensor.record(value);
  }

  public double getIdleTime() {
    return idleTimeSensor.getRate();
  }

  public WritePathLatencySensor getProducerSourceBrokerLatencySensor() {
    return producerSourceBrokerLatencySensor;
  }

  public void recordProducerSourceBrokerLatencyMs(double value, long currentTimeMs) {
    producerSourceBrokerLatencySensor.record(value, currentTimeMs);
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(double value, long currentTimeMs) {
    sourceBrokerLeaderConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getSourceBrokerLeaderConsumerLatencySensor() {
    return sourceBrokerLeaderConsumerLatencySensor;
  }

  public void recordProducerLocalBrokerLatencyMs(double value, long currentTimeMs) {
    producerLocalBrokerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerLocalBrokerLatencySensor() {
    return producerLocalBrokerLatencySensor;
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(double value, long currentTimeMs) {
    localBrokerFollowerConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getLocalBrokerFollowerConsumerLatencySensor() {
    return localBrokerFollowerConsumerLatencySensor;
  }

  public void recordLeaderProducerCompletionLatencyMs(double value, long currentTimeMs) {
    leaderProducerCompletionLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getLeaderProducerCompletionLatencySensor() {
    return leaderProducerCompletionLatencySensor;
  }

  public void recordBatchProcessingRequest(int size, long currentTimeMs) {
    batchProcessingRequestSensor.record();
    batchProcessingRequestRecordsSensor.record(size);
    batchProcessingRequestSizeSensor.record(size, currentTimeMs);
  }

  public double getBatchProcessingRequest() {
    return batchProcessingRequestSensor.getRate();
  }

  public double getBatchProcessingRequestRecords() {
    return batchProcessingRequestRecordsSensor.getRate();
  }

  public void recordBatchProcessingRequestError() {
    batchProcessingRequestErrorSensor.record();
  }

  public double getBatchProcessingRequestError() {
    return batchProcessingRequestErrorSensor.getRate();
  }

  public WritePathLatencySensor getBatchProcessingRequestSizeSensor() {
    return batchProcessingRequestSizeSensor;
  }

  public void recordBatchProcessingRequestLatency(double latency, long currentTimeMs) {
    batchProcessingRequestLatencySensor.record(latency, currentTimeMs);
  }

  public WritePathLatencySensor getBatchProcessingRequestLatencySensor() {
    return batchProcessingRequestLatencySensor;
  }

  public static double unAvailableToZero(double value) {
    /* When data is unavailable, return 0 instead of NaN or Infinity. Some metrics are initialized to -INF.
     This can cause problems when metrics are aggregated. Use only when zero makes semantic sense.
    */
    return Double.isFinite(value) ? value : 0;
  }

  /**
   * Retrieves the storage quota usage for the current ingestion task.
   *
   * @return The disk quota usage as a double value, or 0 if unavailable.
   */
  public double getStorageQuotaUsed() {
    return IngestionStatsUtils.getStorageQuotaUsed(ingestionTask);
  }
}
