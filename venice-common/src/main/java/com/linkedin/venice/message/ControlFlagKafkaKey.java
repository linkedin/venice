package com.linkedin.venice.message;

public class ControlFlagKafkaKey extends KafkaKey{

    private int jobId;

    public ControlFlagKafkaKey(OperationType opType, byte[] key, int jobId) {
        super(opType, key);
        this.jobId = jobId;
    }

    public int getJobId(){
        return jobId;
    }
}
