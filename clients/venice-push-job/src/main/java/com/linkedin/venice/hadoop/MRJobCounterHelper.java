package com.linkedin.venice.hadoop;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;


/**
 * All MapReducer job counters (identified by a group name + a counter name) are defined here and this util class
 * provides method to increment counters and read counters
 */
public class MRJobCounterHelper {
  private static final String COUNTER_GROUP_QUOTA = "Quota";
  private static final String COUNTER_TOTAL_KEY_SIZE = "total key size";
  private static final String COUNTER_TOTAL_UNCOMPRESSED_VALUE_SIZE = "total uncompressed value size";
  private static final String COUNTER_TOTAL_VALUE_SIZE = "total compressed value size stored"; // Compressed using
                                                                                               // NO_OP/GZIP/ZSTD_WITH_DICT
                                                                                               // Compressor
  private static final String COUNTER_TOTAL_GZIP_COMPRESSED_VALUE_SIZE = "total GZIP compressed value size";
  private static final String COUNTER_TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE =
      "total ZSTD with dictionary compressed value size";

  private static final String COUNTER_GROUP_KAFKA = "Kafka";
  private static final String COUNTER_OUTPUT_RECORDS = "Output Records";
  private static final String EMPTY_RECORD = "empty record";
  private static final String AUTHORIZATION_FAILURES = "authorization failures";
  private static final String RECORD_TOO_LARGE_FAILURES = "record too large failures";

  private static final String COUNTER_GROUP_DATA_QUALITY = "Data quality";
  private static final String DUPLICATE_KEY_WITH_IDENTICAL_VALUE = "duplicate key with identical value";
  private static final String DUPLICATE_KEY_WITH_DISTINCT_VALUE = "duplicate key with distinct value";

  private static final String MR_JOB_STATUS = "MapReduce job status";
  /** Info Counters */
  private static final String REDUCER_JOB_CLOSED_COUNT = "Reducer job closed count";
  private static final String MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT =
      "Mapper spray all partitions triggered count";
  private static final String MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED = "Mapper num records successfully processed";
  private static final String MAPPER_ZSTD_DICT_TRAIN_SUCCESS = "Mapper Zstd dict train success";
  /** Error Counters */
  private static final String MAPPER_ERROR_DATA_MODIFIED_DURING_PUSH_JOB =
      "Mapper Error: Data modified during push job";
  private static final String MAPPER_INVALID_INPUT_IDX = "Mapper invalid input index";
  private static final String MAPPER_INVALID_INPUT_FILE = "Mapper invalid input file";
  private static final String MAPPER_SCHEMA_INCONSISTENCY_FAILURE = "Mapper schema inconsistency failure";
  private static final String MAPPER_ZSTD_DICT_TRAIN_FAILURE = "Mapper Zstd dict train failure";
  private static final String MAPPER_ZSTD_DICT_TRAIN_SKIPPED = "Mapper Zstd dict train skipped";
  private static final String COUNTER_GROUP_KAFKA_INPUT_FORMAT = "KafkaInputFormat";
  private static final String COUNTER_PUT_OR_DELETE_RECORDS = "put or delete records";

  private static final String REPUSH_TTL_FILTERED_COUNT = "Repush ttl filtered count";

  static final GroupAndCounterNames WRITE_ACL_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, AUTHORIZATION_FAILURES);

  static final GroupAndCounterNames TOTAL_KEY_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE);

  static final GroupAndCounterNames TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_VALUE_SIZE);

  static final GroupAndCounterNames TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_UNCOMPRESSED_VALUE_SIZE);

  static final GroupAndCounterNames TOTAL_GZIP_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_GZIP_COMPRESSED_VALUE_SIZE);

  static final GroupAndCounterNames TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE);

  static final GroupAndCounterNames DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, DUPLICATE_KEY_WITH_IDENTICAL_VALUE);

  static final GroupAndCounterNames DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, DUPLICATE_KEY_WITH_DISTINCT_VALUE);

  static final GroupAndCounterNames RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, RECORD_TOO_LARGE_FAILURES);

  static final GroupAndCounterNames OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, COUNTER_OUTPUT_RECORDS);

  static final GroupAndCounterNames EMPTY_RECORD_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, EMPTY_RECORD);

  static final GroupAndCounterNames REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, REDUCER_JOB_CLOSED_COUNT);

  static final GroupAndCounterNames MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT);

  static final GroupAndCounterNames TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA_INPUT_FORMAT, COUNTER_PUT_OR_DELETE_RECORDS);

  static final GroupAndCounterNames MAPPER_ERROR_DATA_MODIFIED_DURING_PUSH_JOB_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_ERROR_DATA_MODIFIED_DURING_PUSH_JOB);

  static final GroupAndCounterNames MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_INVALID_INPUT_IDX);

  static final GroupAndCounterNames MAPPER_INVALID_INPUT_FILE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_INVALID_INPUT_FILE);

  static final GroupAndCounterNames MAPPER_SCHEMA_INCONSISTENCY_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_SCHEMA_INCONSISTENCY_FAILURE);

  static final GroupAndCounterNames MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_ZSTD_DICT_TRAIN_SUCCESS);

  static final GroupAndCounterNames MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_ZSTD_DICT_TRAIN_FAILURE);

  static final GroupAndCounterNames MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_ZSTD_DICT_TRAIN_SKIPPED);

  static final GroupAndCounterNames MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED);

  static final GroupAndCounterNames REPUSH_TTL_FILTER_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, REPUSH_TTL_FILTERED_COUNT);

  private MRJobCounterHelper() {
    // Util class
  }

  static void incrReducerClosedCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, amount);
  }

  static void incrMapperSprayAllPartitionsTriggeredCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, amount);
  }

  static void incrWriteAclAuthorizationFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  static void incrRecordTooLargeFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  static void incrTotalKeySize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_KEY_SIZE_GROUP_COUNTER_NAME, amount);
  }

  static void incrTotalValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  static void incrTotalUncompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  static void incrTotalGzipCompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_GZIP_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  static void incrTotalZstdCompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  static void incrDuplicateKeyWithIdenticalValue(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME, amount);
  }

  static void incrDuplicateKeyWithDistinctValue(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, amount);
  }

  static void incrOutputRecordCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME, amount);
  }

  static void incrEmptyRecordCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, EMPTY_RECORD_COUNTER_NAME, amount);
  }

  public static void incrTotalPutOrDeleteRecordCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME, amount);
  }

  static long getWriteAclAuthorizationFailureCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, WRITE_ACL_FAILURE_GROUP_COUNTER_NAME);
  }

  static long getDuplicateKeyWithDistinctCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME);
  }

  static long getRecordTooLargeFailureCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME);
  }

  static long getTotalKeySize(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, TOTAL_KEY_SIZE_GROUP_COUNTER_NAME);
  }

  static long getTotalValueSize(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalPutOrDeleteRecordsCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME);
  }

  static long getReducerClosedCount(Counters counters) {
    return getCountFromCounters(counters, REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME);
  }

  static long getMapperSprayAllPartitionsTriggeredCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME);
  }

  static long getOutputRecordsCount(Counters counters) {
    return getCountFromCounters(counters, OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME);
  }

  static long getWriteAclAuthorizationFailureCount(Counters counters) {
    return getCountFromCounters(counters, WRITE_ACL_FAILURE_GROUP_COUNTER_NAME);
  }

  static long getDuplicateKeyWithDistinctCount(Counters counters) {
    return getCountFromCounters(counters, DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME);
  }

  static long getRecordTooLargeFailureCount(Counters counters) {
    return getCountFromCounters(counters, RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME);
  }

  static long getTotalKeySize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_KEY_SIZE_GROUP_COUNTER_NAME);
  }

  static long getTotalValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  static long getTotalUncompressedValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  static long getTotalGzipCompressedValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_GZIP_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  static long getTotalZstdWithDictCompressedValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  static long getTotalPutOrDeleteRecordsCount(Counters counters) {
    return getCountFromCounters(counters, TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME);
  }

  private static long getCountFromCounters(Counters counters, GroupAndCounterNames groupAndCounterNames) {
    if (counters == null) {
      return 0;
    }
    return counters.getGroup(groupAndCounterNames.getGroupName()).getCounter(groupAndCounterNames.getCounterName());
  }

  private static long getCountWithGroupCounterName(Reporter reporter, GroupAndCounterNames groupAndCounterNames) {
    if (reporter == null) {
      return 0;
    }
    return reporter.getCounter(groupAndCounterNames.getGroupName(), groupAndCounterNames.getCounterName()).getCounter();
  }

  private static void incrAmountWithGroupCounterName(
      Reporter reporter,
      GroupAndCounterNames groupAndCounterNames,
      long amount) {
    if (reporter == null || amount == 0) {
      return;
    }
    reporter.incrCounter(groupAndCounterNames.getGroupName(), groupAndCounterNames.getCounterName(), amount);
  }

  static void incrMapperErrorDataModifiedDuringPushJobCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_ERROR_DATA_MODIFIED_DURING_PUSH_JOB_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperErrorDataModifiedDuringPushJobCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_ERROR_DATA_MODIFIED_DURING_PUSH_JOB_GROUP_COUNTER_NAME);
  }

  static void incrMapperInvalidInputIdxCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperInvalidInputIdxCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME);
  }

  static void incrMapperInvalidInputFileCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_INVALID_INPUT_FILE_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperInvalidInputFileCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_INVALID_INPUT_FILE_GROUP_COUNTER_NAME);
  }

  static void incrMapperSchemaInconsistencyFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_SCHEMA_INCONSISTENCY_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperSchemaInconsistencyFailureCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_SCHEMA_INCONSISTENCY_FAILURE_GROUP_COUNTER_NAME);
  }

  static void incrMapperZstdDictTrainSuccessCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperZstdDictTrainSuccessCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME);
  }

  static void incrMapperZstdDictTrainFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperZstdDictTrainFailureCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME);
  }

  static void incrMapperZstdDictTrainSkippedCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperZstdDictTrainSkippedCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME);
  }

  static void incrMapperNumRecordsSuccessfullyProcessedCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME, amount);
  }

  static long getMapperNumRecordsSuccessfullyProcessedCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME);
  }

  public static long getRepushTtlFilterCount(Counters counters) {
    return getCountFromCounters(counters, REPUSH_TTL_FILTER_COUNT_GROUP_COUNTER_NAME);
  }

  public static void incrRepushTtlFilterCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, REPUSH_TTL_FILTER_COUNT_GROUP_COUNTER_NAME, amount);
  }

  /**
   * Bundle counter group name and counter name in this POJO
   */
  static class GroupAndCounterNames {
    private final String groupName;
    private final String counterName;

    GroupAndCounterNames(String groupName, String counterName) {
      this.groupName = groupName;
      this.counterName = counterName;
    }

    String getGroupName() {
      return groupName;
    }

    String getCounterName() {
      return counterName;
    }
  }
}
