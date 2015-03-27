package com.linkedin.venice.message;

import com.linkedin.venice.exceptions.VeniceMessageException;

/**
 * Enum which declares the possible types of operation supported by Venice.
 *
 * For Kafka key header we want both PUT and DELETE to share the same code. Hence introduced another enum called WRITE.
 * This is necessary for log compaction in kafka logs
 *
 * For Kafka value, we need ways to distinguish between PUT and DELETE. So we have separate enums for them.
 */
public enum OperationType {
    WRITE, PARTIAL_WRITE, PUT, DELETE, BEGIN_OF_PUSH, END_OF_PUSH;

    public static byte getByteCode(OperationType operationType) {
        switch (operationType) {
            case WRITE:
                return 0;
            case PARTIAL_WRITE:
                return 1;
            case PUT:
                return 2;
            case DELETE:
                return 3;
            case BEGIN_OF_PUSH:
                return 4;
            case END_OF_PUSH:
                return 5;
            default:
                throw new VeniceMessageException("Invalid operation type: " + operationType.toString());
        }
    }

    public static OperationType getOperationType(byte opCode) {
        switch (opCode) {
            case 0:
                return WRITE;
            case 1:
                return PARTIAL_WRITE;
            case 2:
                return PUT;
            case 3:
                return DELETE;
            case 4:
                return BEGIN_OF_PUSH;
            case 5:
                return END_OF_PUSH;
            default:
                throw new VeniceMessageException("Invalid opcode for operation type: " + opCode);
        }
    }
}
