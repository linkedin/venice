package com.linkedin.venice.migration;

public enum MigrationPushStrategy {
  RunBnPOnlyStrategy, RunVPJOnlyStrategy, RunBnPAndVPJWaitForBothStrategy, RunBnPAndVPJWaitForBnPOnlyStrategy,
  RunBnPAndVPJWaitForVPJOnlyStrategy;

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
