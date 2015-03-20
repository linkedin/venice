package com.linkedin.venice.message;

import com.linkedin.venice.exceptions.VeniceMessageException;

/**
 * Enum which declares the possible types of operation supported by Venice.
 */
public enum OperationType {
  PUT, DELETE, PARTIAL_PUT;

  public static byte toByte(OperationType operationType) {
    switch (operationType) {
      case PUT: return 1;
      case DELETE: return 2;
      case PARTIAL_PUT: return 3;
      default: throw new VeniceMessageException("Invalid operation type: " + operationType.toString());
    }
  }

  public static OperationType fromByte(byte opCode) {
    switch (opCode) {
      case 1: return PUT;
      case 2: return DELETE;
      case 3: return PARTIAL_PUT;
      default: throw new VeniceMessageException("Invalid opcode for operation type: " + opCode);
    }
  }
}
