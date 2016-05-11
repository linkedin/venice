package com.linkedin.venice.serialization.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerRecord;


public class JobProgressKafkaRecord {

    public final static String AVRO_KEY_STORE_INFO = "{\n" +
            "    \"type\": \"record\",\n" +
            "    \"namespace\": \"com.linkedin.venice.internal\",\n" +
            "    \"name\": \"AckPartitionConsumptionKey\",\n" +
            "    \"fields\": [{\n" +
            "        \"name\": \"jobId\",\n" +
            "        \"type\": \"long\",\n" +
            "        \"doc\": \"This points to the azkaban job id\"\n" +
            "    }, {\n" +
            "        \"name\": \"kafka-topic\",\n" +
            "        \"type\": \"string\",\n" +
            "        \"doc\": \"Refers to the kafka topic name which is also same as the Venice store name\"\n" +
            "    }, {\n" +
            "        \"name\": \"partitionId\",\n" +
            "        \"type\": \"int\",\n" +
            "        \"doc\": \"This is the kafka partition id for which the consumption is done.\"\n" +
            "    }, {\n" +
            "        \"name\": \"nodeId\",\n" +
            "        \"type\": \"int\",\n" +
            "        \"doc\": \"This is the id of the node which is sending the ack.\"\n" +
            "    }]\n" +
            "}";
    public final static String AVRO_VALUE_PROGRESS = "{\n" +
            "    \"type\": \"record\",\n" +
            "    \"namespace\": \"com.linkedin.venice.internal\",\n" +
            "    \"name\": \"AckPartitionConsumptionValue\",\n" +
            "    \"fields\": [{\n" +
            "        \"name\": \"count\",\n" +
            "        \"type\": \"long\",\n" +
            "        \"doc\": \"This refers to the number of messages consumed from the given kafka partition and topic\"\n" +
            "    }]\n" +
            "}";

    private final AvroGenericSerializer keySerializer;
    private final AvroGenericSerializer valueSerializer;
    private final String consumptionAckKafkaTopic;

    public JobProgressKafkaRecord(String consumptionAckKafkaTopic){
        this.consumptionAckKafkaTopic = consumptionAckKafkaTopic;
        keySerializer = new AvroGenericSerializer(AVRO_KEY_STORE_INFO);
        valueSerializer = new AvroGenericSerializer(AVRO_VALUE_PROGRESS);
    }

    /**
     * 1. Gets the following input params
     * 2. Creates a key and value Avro record.
     * 3. Serailizes the avro records - both key and value.
     * 4. Constructs and returns a KeyedMessage using the serialized key and value avro records
     *
     * @param kafkaTopic
     * @param partitionId
     * @param nodeId
     * @param count
     * @return
     */
    public ProducerRecord<byte[], byte[]> generate(String kafkaTopic, int partitionId, int nodeId,
            long count){
        GenericData.Record keyRecord = constructKeyRecord(kafkaTopic,partitionId, nodeId);
        GenericData.Record valRecord = constructValueRecord(count);

        byte[] keyBytes = keySerializer.serialize(kafkaTopic, keyRecord);
        byte[] valBytes = valueSerializer.serialize(kafkaTopic, valRecord);

        ProducerRecord<byte[], byte[]> kafkaMessage = new ProducerRecord<byte[], byte[]>(consumptionAckKafkaTopic, keyBytes, valBytes);
        return kafkaMessage;
    }

    private GenericData.Record constructKeyRecord(String kafkaTopic, int partitionId, int nodeId){
        GenericData.Record key = new GenericData.Record(Schema.parse(AVRO_KEY_STORE_INFO));
        key.put("jobId", -1); // TODO : This should be replaced by Producer GUID and not long.
        key.put("kafka-topic", new Utf8(kafkaTopic));
        key.put("partitionId", partitionId);
        key.put("nodeId", nodeId);
        return key;
    }

    private GenericData.Record constructValueRecord(long count){
        GenericData.Record val = new GenericData.Record(Schema.parse(AVRO_VALUE_PROGRESS));
        val.put("count", count);
        return val;
    }


}
