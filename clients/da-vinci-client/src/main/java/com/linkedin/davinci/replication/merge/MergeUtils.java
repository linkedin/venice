package com.linkedin.davinci.replication.merge;

public class MergeUtils {
  private MergeUtils() {
    // Utility class
  }

  /**
   * @return the object with a higher hash code.
   */
  public static Object compareAndReturn(Object o1, Object o2) {
    // nulls win comparison on the basis that we prefer deletes to win in the case
    // where there is a tie on RMD timestamp comparison.
    if (o1 == null) {
      return o1;
    } else if (o2 == null) {
      return o2;
    }
    // for same object always return first object o1.
    // TODO: note that hashCode based compare and return is not deterministic when hash collision happens. So, it should
    // be migrated to use {@link AvroCollectionElementComparator} later.
    if (o1.hashCode() >= o2.hashCode()) {
      return o1;
    } else {
      return o2;
    }
  }
}
