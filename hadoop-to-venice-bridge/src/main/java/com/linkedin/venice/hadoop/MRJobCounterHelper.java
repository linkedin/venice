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
  private static final String COUNTER_TOTAL_VALUE_SIZE = "total value size";

  private static final String COUNTER_GROUP_KAFKA = "Kafka";
  private static final String COUNTER_OUTPUT_RECORDS = "Output Records";
  private static final String EMPTY_RECORD = "empty record";
  private static final String AUTHORIZATION_FAILURES = "authorization failures";
  private static final String RECORD_TOO_LARGE_FAILURES = "record too large failures";

  private static final String COUNTER_GROUP_DATA_QUALITY = "Data quality";
  private static final String DUPLICATE_KEY_WITH_IDENTICAL_VALUE = "duplicate key with identical value";
  private static final String DUPLICATE_KEY_WITH_DISTINCT_VALUE = "duplicate key with distinct value";

  private static final String MR_JOB_STATUS = "MapReduce job status";
  private static final String REDUCER_JOB_CLOSED_COUNT = "Reducer job closed count";

  private static final String KAFKA_PRODUCER_METRIC_FOR_BROKER = "%s for %s";
  static final String COUNTER_GROUP_KAFKA_BROKER = "Kafka metrics aggregated per broker";

  static final GroupAndCounterNames WRITE_ACL_FAILURE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_KAFKA, AUTHORIZATION_FAILURES);

  static final GroupAndCounterNames TOTAL_KEY_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE);

  static final GroupAndCounterNames TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_VALUE_SIZE);

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

  static final GroupAndCounterNames TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_UNCOMPRESSED_VALUE_SIZE);

  static final GroupAndCounterNames REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME =
      new GroupAndCounterNames(MR_JOB_STATUS, REDUCER_JOB_CLOSED_COUNT);

  private MRJobCounterHelper() {
    // Util class
  }

  static void incrReducerClosedCount(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME, amount);
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

  static void incrTotalUncompressedValueSize(Reporter reporter, long amount) {
    incrAmountWithGroupCounterName(reporter, TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME, amount);
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

  static long getReducerClosedCount(Counters counters) {
    return getCountFromCounters(counters, REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME);
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

  private static void incrAmountWithGroupCounterName(Reporter reporter, GroupAndCounterNames groupAndCounterNames, long amount) {
    if (reporter == null || amount == 0) {
      return;
    }
    reporter.incrCounter(groupAndCounterNames.getGroupName(), groupAndCounterNames.getCounterName(), amount);
  }

  public static String getKafkaProducerMetricForBrokerCounterName(String metric, String broker) {
    return String.format(KAFKA_PRODUCER_METRIC_FOR_BROKER, metric, broker);
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
