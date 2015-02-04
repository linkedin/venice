package com.linkedin.venice.message;

/**
 * Enum which declares the possible types of operation supported by Venice.
 */
public enum OperationType {
  PUT, DELETE, PARTIAL_PUT ;

    private static final OperationType values[] =values();

    public static OperationType getOperationType(int value) {
        return values[value];
    }
}
