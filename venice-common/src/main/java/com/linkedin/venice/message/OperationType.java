package com.linkedin.venice.message;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


/**
 * Enum which declares the possible types of operation supported by Venice.
 *
 * For Kafka key header we want both PUT and DELETE to share the same code. Hence introduced another enum called WRITE.
 * This is necessary for log compaction in kafka logs
 *
 * For Kafka value, we need ways to distinguish between PUT and DELETE. So we have separate enums for them.
 */
public enum OperationType {
  // TODO: The below enum types are mixed and matched. Some of them are valid
  // only for KafkaKey and some others are valid for KafkaValue. There are two
  // ways to clean up, create pairs of what can go to gether or use two different
  // enums.
  WRITE(0), PARTIAL_WRITE(1), PUT(2), DELETE(3), BEGIN_OF_PUSH(4), END_OF_PUSH(5);

  private byte value;

  OperationType(int value) {
    this.value = (byte) value;
  }

  public static byte getValue(OperationType operationType) {
    return operationType.value;
  }

  private static final Map<Byte, OperationType> intToTypeMap = new HashMap<>();

  static {
    for (OperationType type : OperationType.values()) {
      intToTypeMap.put(type.value, type);
    }
  }

  public static OperationType getOperationType(byte value) {
    OperationType type = intToTypeMap.get(Byte.valueOf(value));
    if (type == null) {
      throw new VeniceMessageException("Invalid opcode for operation type: " + value);
    }
    return type;
  }

  public static boolean isControlOperation(OperationType type) {
    return type == BEGIN_OF_PUSH || type == END_OF_PUSH;
  }

}
