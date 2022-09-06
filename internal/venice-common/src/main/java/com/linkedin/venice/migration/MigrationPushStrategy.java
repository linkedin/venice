package com.linkedin.venice.migration;

public enum MigrationPushStrategy {
  RunBnPOnlyStrategy, RunH2VOnlyStrategy, RunBnPAndH2VWaitForBothStrategy, RunBnPAndH2VWaitForBnPOnlyStrategy,
  RunBnPAndH2VWaitForH2VOnlyStrategy;

  public static String getAllEnumString() {
    StringBuilder sb = new StringBuilder();
    for (MigrationPushStrategy e: values()) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(e.toString());
    }

    return sb.toString();
  }
}
