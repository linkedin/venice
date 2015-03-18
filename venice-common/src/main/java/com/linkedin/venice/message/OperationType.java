package com.linkedin.venice.message;

/**
 * Enum which declares the possible types of operation supported by Venice.
 */
public enum OperationType {
  PUT, DELETE, PARTIAL_PUT, ERROR;

  public static byte toByte(OperationType operationType) {
    switch (operationType) {
      case PUT: return 1;
      case DELETE: return 2;
      case PARTIAL_PUT: return 3;
      default: return 0;
    }
  }

  public static OperationType fromByte(byte opCode) {
    switch (opCode) {
      case 1: return PUT;
      case 2: return DELETE;
      case 3: return PARTIAL_PUT;
      default: return ERROR;
    }
  }
}
