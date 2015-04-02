package com.linkedin.venice.message;

public class ControlFlagKafkaKey extends KafkaKey{

    private long jobId;

    public ControlFlagKafkaKey(OperationType opType, byte[] key, long jobId) {
        super(opType, key);
        this.jobId = jobId;
    }

    public long getJobId(){
        return jobId;
    }
}
