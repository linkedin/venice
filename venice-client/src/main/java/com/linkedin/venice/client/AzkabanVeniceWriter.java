package com.linkedin.venice.client;

import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.Serializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Props;


public class AzkabanVeniceWriter<K, V> extends VeniceWriter<K, V> {

    private final String JOB_ID = "azkaban.flow.execid";
    private final String KAFKA_NUM_PARTITIONS = "num.partitions.in.kafka";

    long jobId;
    int numPartitions;

    public AzkabanVeniceWriter(Props props, String storeName, Serializer keySerializer, Serializer valueSerializer) {
        super(props, storeName, keySerializer, valueSerializer);
        jobId = props.getLong(JOB_ID);
        numPartitions = props.getInt(KAFKA_NUM_PARTITIONS);
    }

    public void broadcastBeginOfPush() {
        broadcast(OperationType.BEGIN_OF_PUSH);
    }

    private void broadcast(OperationType opType){
        KafkaKey key;
        KafkaValue value;
        byte[] partitionId = new byte[ByteUtils.SIZE_OF_INT];
        for (int i = 0; i < numPartitions; i++) {
            ByteUtils.writeInt(partitionId, i, 0);
            key = new ControlFlagKafkaKey(opType, partitionId, jobId);
            value = new KafkaValue(opType);
            producer.sendMessage(storeName,key,value);
        }
    }

    public void broadcastEndOfPush() {
        broadcast(OperationType.END_OF_PUSH);
    }
}
