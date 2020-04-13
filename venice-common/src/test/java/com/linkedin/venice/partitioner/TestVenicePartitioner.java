package com.linkedin.venice.partitioner;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.PartitionInfo;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Venice Partitioner. It doesn't really matter what the results are, as long as they are consistent.
 * 1. Testing several strings and ensuring that they are partitioned consistently
 * - numeric
 * - special characters
 * - white space
 */
public class TestVenicePartitioner {

    @Test
    public void testConsistentPartitioning() {

        VenicePartitioner vp = new DefaultVenicePartitioner();

        byte[] keyBytes = "key1".getBytes();
        KafkaKey key = new KafkaKey(MessageType.PUT, keyBytes);  // OperationType doesn't matter. We are just testing the partitioning.

        PartitionInfo [] partitionArray = {new PartitionInfo("", 0, null, null, null),
            new PartitionInfo("", 1, null, null, null), new PartitionInfo("", 2, null, null, null)};

        // Test 1
        int partition1 = vp.getPartitionId(keyBytes, partitionArray.length);
        int partition2 = vp.getPartitionId(keyBytes, partitionArray.length);
        Assert.assertEquals(partition1, partition2);

        // Test 2
      keyBytes = "    ".getBytes();
        partition1 = vp.getPartitionId(keyBytes, partitionArray.length);
        partition2 = vp.getPartitionId(keyBytes, partitionArray.length);
        Assert.assertEquals(partition1, partition2);

        // Test 3
        keyBytes = "00000".getBytes();
        partition1 = vp.getPartitionId(keyBytes, partitionArray.length);
        partition2 = vp.getPartitionId(keyBytes, partitionArray.length);
        Assert.assertEquals(partition1, partition2);

    }

  @Test
  public void testEntityUrnPartitioning() {
    VenicePartitioner vp = new EntityUrnPartitioner();

    long key = 888L;
    int numPartitions = 16;
    Schema schema = Schema.parse(
          "{"
            + "\"type\":\"record\", "
            + "\"name\":\"PartitionKey\", "
            + "\"fields\": ["
              + "{"
                + "\"name\":\"entityUrn\", "
                + "\"type\":\"string\""
              + "},"
              + "{"
                + "\"name\":\"version\", "
                + "\"type\":\"string\""
              + "}"
            + "]"
          + "}");

    GenericData.Record testRecord = new GenericData.Record(schema);
    testRecord.put("entityUrn", "urn:li:jobPosting:888");
    testRecord.put("version", "abc");
    RecordSerializer<Object> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(schema);
    byte[] serializedKey = serializer.serialize(testRecord);

    int partition1 = vp.getPartitionId(serializedKey, numPartitions);
    int partition2 = vp.getPartitionId(ByteBuffer.wrap(serializedKey), numPartitions);
    Assert.assertEquals(partition1, key % numPartitions);
    Assert.assertEquals(partition2, key % numPartitions);
  }
}
