package com.linkedin.venice.hadoop.mapreduce.counter;

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
  private static final String UNCOMPRESSED_RECORD_TOO_LARGE_FAILURES = "uncompressed record too large failures";
  private static final String INCREMENTAL_PUSH_THROTTLE_TIME_MS = "incremental push throttle time (ms)";

  private static final String COUNTER_GROUP_DATA_QUALITY = "Data quality";
  private static final String DUPLICATE_KEY_WITH_IDENTICAL_VALUE = "duplicate key with identical value";
  private static final String DUPLICATE_KEY_WITH_DISTINCT_VALUE = "duplicate key with distinct value";

  private static final String MR_JOB_STATUS = "MapReduce job status";
  /** Info Counters */
  private static final String REDUCER_JOB_CLOSED_COUNT = "Reducer job closed count";
  private static final String MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT =
      "Mapper spray all partitions triggered count";
  private static final String COUNTER_GROUP_KAFKA_INPUT_FORMAT = "KafkaInputFormat";
  private static final String COUNTER_PUT_OR_DELETE_RECORDS = "put or delete records";

  private static final String REPUSH_TTL_FILTERED_COUNT = "Repush ttl filtered count";

  public static final GroupAndCounterNames WRITE_ACL_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, AUTHORIZATION_FAILURES);

  public static final GroupAndCounterNames TOTAL_KEY_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE);

  public static final GroupAndCounterNames TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_VALUE_SIZE);

  public static final GroupAndCounterNames TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_UNCOMPRESSED_VALUE_SIZE);

  public static final GroupAndCounterNames TOTAL_GZIP_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_GZIP_COMPRESSED_VALUE_SIZE);

  public static final GroupAndCounterNames TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE);

  public static final GroupAndCounterNames DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, DUPLICATE_KEY_WITH_IDENTICAL_VALUE);

  public static final GroupAndCounterNames DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, DUPLICATE_KEY_WITH_DISTINCT_VALUE);

  public static final GroupAndCounterNames RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, RECORD_TOO_LARGE_FAILURES);

  public static final GroupAndCounterNames UNCOMPRESSED_RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_DATA_QUALITY, UNCOMPRESSED_RECORD_TOO_LARGE_FAILURES);

  public static final GroupAndCounterNames OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, COUNTER_OUTPUT_RECORDS);

  public static final GroupAndCounterNames EMPTY_RECORD_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, EMPTY_RECORD);

  public static final GroupAndCounterNames REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, REDUCER_JOB_CLOSED_COUNT);

  public static final GroupAndCounterNames MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT);

  public static final GroupAndCounterNames TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA_INPUT_FORMAT, COUNTER_PUT_OR_DELETE_RECORDS);

  public static final GroupAndCounterNames REPUSH_TTL_FILTER_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, REPUSH_TTL_FILTERED_COUNT);

  public static final GroupAndCounterNames INCREMENTAL_PUSH_THROTTLE_TIME_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, INCREMENTAL_PUSH_THROTTLE_TIME_MS);

  private MRJobCounterHelper() {
    // Util class
  }

  public static void incrReducerClosedCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, amount);
  }

  public static void incrMapperSprayAllPartitionsTriggeredCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME, amount);
  }

  public static void incrWriteAclAuthorizationFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, WRITE_ACL_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrRecordTooLargeFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrUncompressedRecordTooLargeFailureCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, UNCOMPRESSED_RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrTotalKeySize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_KEY_SIZE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrTotalValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrTotalUncompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrTotalGzipCompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_GZIP_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrTotalZstdCompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrDuplicateKeyWithIdenticalValue(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrDuplicateKeyWithDistinctValue(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME, amount);
  }

  public static void incrOutputRecordCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME, amount);
  }

  public static void incrEmptyRecordCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, EMPTY_RECORD_COUNTER_NAME, amount);
  }

  public static void incrTotalPutOrDeleteRecordCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME, amount);
  }

  public static void incrIncrementalPushThrottleTime(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, INCREMENTAL_PUSH_THROTTLE_TIME_GROUP_COUNTER_NAME, amount);
  }

  public static long getWriteAclAuthorizationFailureCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, WRITE_ACL_FAILURE_GROUP_COUNTER_NAME);
  }

  public static long getDuplicateKeyWithDistinctCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME);
  }

  public static long getRecordTooLargeFailureCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME);
  }

  public static long getUncompressedRecordTooLargeFailureCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, UNCOMPRESSED_RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME);
  }

  public static long getTotalKeySize(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, TOTAL_KEY_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalValueSize(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalPutOrDeleteRecordsCount(Reporter reporter) {
    return getCountWithGroupCounterName(reporter, TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME);
  }

  public static long getReducerClosedCount(Counters counters) {
    return getCountFromCounters(counters, REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME);
  }

  public static long getMapperSprayAllPartitionsTriggeredCount(Counters counters) {
    return getCountFromCounters(counters, MAPPER_SPRAY_ALL_PARTITIONS_TRIGGERED_COUNT_NAME);
  }

  public static long getOutputRecordsCount(Counters counters) {
    return getCountFromCounters(counters, OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME);
  }

  public static long getWriteAclAuthorizationFailureCount(Counters counters) {
    return getCountFromCounters(counters, WRITE_ACL_FAILURE_GROUP_COUNTER_NAME);
  }

  public static long getDuplicateKeyWithDistinctCount(Counters counters) {
    return getCountFromCounters(counters, DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME);
  }

  public static long getRecordTooLargeFailureCount(Counters counters) {
    return getCountFromCounters(counters, RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME);
  }

  public static long getUncompressedRecordTooLargeFailureCount(Counters counters) {
    return getCountFromCounters(counters, UNCOMPRESSED_RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME);
  }

  public static long getTotalKeySize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_KEY_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalUncompressedValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalGzipCompressedValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_GZIP_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalZstdWithDictCompressedValueSize(Counters counters) {
    return getCountFromCounters(counters, TOTAL_ZSTD_WITH_DICT_COMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME);
  }

  public static long getTotalPutOrDeleteRecordsCount(Counters counters) {
    return getCountFromCounters(counters, TOTAL_PUT_OR_DELETE_COUNT_GROUP_COUNTER_NAME);
  }

  public static long getIncrementalPushThrottleTimeMs(Counters counters) {
    return getCountFromCounters(counters, INCREMENTAL_PUSH_THROTTLE_TIME_GROUP_COUNTER_NAME);
  }

  private static long getCountFromCounters(Counters counters, GroupAndCounterNames groupAndCounterNames) {
    if (counters == null) {
      return 0;
    }
    return counters.getGroup(groupAndCounterNames.getGroupName()).getCounter(groupAndCounterNames.getCounterName());
  }

  private static long getCountWithGroupCounterName(Reporter reporter, GroupAndCounterNames groupAndCounterNames) {
    if (reporter == null || reporter.equals(Reporter.NULL)) {
      return 0;
    }
    return reporter.getCounter(groupAndCounterNames.getGroupName(), groupAndCounterNames.getCounterName()).getCounter();
  }

  private static void incrAmountWithGroupCounterName(
      Reporter reporter,
      GroupAndCounterNames groupAndCounterNames,
      long amount) {
    if (reporter == null || reporter.equals(Reporter.NULL) || amount == 0) {
      return;
    }
    reporter.incrCounter(groupAndCounterNames.getGroupName(), groupAndCounterNames.getCounterName(), amount);
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
  public static class GroupAndCounterNames {
    private final String groupName;
    private final String counterName;

    public GroupAndCounterNames(String groupName, String counterName) {
      this.groupName = groupName;
      this.counterName = counterName;
    }

    public String getGroupName() {
      return groupName;
    }

    public String getCounterName() {
      return counterName;
    }
  }
}
