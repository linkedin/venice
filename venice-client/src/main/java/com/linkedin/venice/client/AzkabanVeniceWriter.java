package com.linkedin.venice.client;

import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.Serializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Props;
import java.util.Properties;
import kafka.utils.VerifiableProperties;


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
        for (int i = 0; i < numPartitions; i++) {
          KafkaKey key;
          KafkaValue value;
          byte[] partitionId = new byte[ByteUtils.SIZE_OF_INT];
            ByteUtils.writeInt(partitionId, i, 0);
            key = new ControlFlagKafkaKey(opType, partitionId, jobId);
            //logger.info("key in broadcast msg: " + ByteUtils.readInt(partitionId, 0));
            value = new KafkaValue(opType);
            producer.sendMessage(storeName,key,value);
        }
    }

    public void broadcastEndOfPush() {
        broadcast(OperationType.END_OF_PUSH);
    }

    public static void main(String args[]){
        Props props = new Props();
        props.put("azkaban.flow.execid", 1234L);
        props.put("num.partitions.in.kafka", 8);
        props.put("kafka.metadata.broker.list", "eat1-kafka-vip-6.corp.linkedin.com:10251");

        AzkabanVeniceWriter writer = new AzkabanVeniceWriter<byte[], byte[]>(props,"venice_location_7", new DefaultSerializer(new VerifiableProperties()),new DefaultSerializer(new VerifiableProperties()));
        writer.broadcastBeginOfPush();

    }
}
