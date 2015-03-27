package com.linkedin.venice.message;

import com.linkedin.venice.exceptions.VeniceMessageException;

/**
 * Class which stores the components of a Kafka Key, and is the format specified in the KafkaKeySerializer
 */
public class KafkaKey {

    private OperationType opType;
    private byte[] key;

    public KafkaKey(OperationType opType, byte[] key) {
        validateOperationType(opType);
        this.opType = opType;
        this.key = key;
    }

    public void validateOperationType(OperationType opType) {
        if (opType == OperationType.PUT || opType == OperationType.DELETE) {
            throw new VeniceMessageException("Invalid operation type: " + opType + "for KafkaKey header. Use WRITE instead");
        }
    }

    public OperationType getOperationType() {
        return opType;
    }

    public byte[] getKey() {
        return key;
    }

    public String toString() {
        return key.toString();
    }
}
