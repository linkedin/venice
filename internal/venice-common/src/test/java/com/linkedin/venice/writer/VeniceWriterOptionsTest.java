package com.linkedin.venice.writer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import org.testng.annotations.Test;


public class VeniceWriterOptionsTest {
  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".*cannot be null.*")
  public void testVeniceWriterOptionsIfTopicNameIsNull() {
    new VeniceWriterOptions.Builder(null).build();
  }

  @Test
  public void testVeniceWriterOptionsFillsInCorrectDefaults() {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder("store_v1").build();
    assertNotNull(options);
    assertEquals(options.getTopicName(), "store_v1");
    assertTrue(options.getKeySerializer() instanceof DefaultSerializer);
    assertTrue(options.getValueSerializer() instanceof DefaultSerializer);
    assertTrue(options.getWriteComputeSerializer() instanceof DefaultSerializer);
    assertTrue(options.getPartitioner() instanceof DefaultVenicePartitioner);
    assertEquals(options.getTime(), SystemTime.INSTANCE);
    assertNull(options.getPartitionCount());
    assertFalse(options.isChunkingEnabled());
    assertFalse(options.isRmdChunkingEnabled());
    assertNull(options.getBrokerAddress());
  }

  @Test
  public void testVeniceWriterOptionsUsesUserSuppliedOptions() {
    VeniceAvroKafkaSerializer valSer = new VeniceAvroKafkaSerializer("\"string\"");
    VeniceAvroKafkaSerializer wcSer = new VeniceAvroKafkaSerializer("\"string\"");
    VenicePartitioner venicePartitioner = new SimplePartitioner();
    Time time = new SystemTime();

    VeniceWriterOptions options = new VeniceWriterOptions.Builder("store_v1").setUseKafkaKeySerializer(true)
        .setValueSerializer(valSer)
        .setWriteComputeSerializer(wcSer)
        .setPartitioner(venicePartitioner)
        .setTime(time)
        .setPartitionCount(20)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .setBrokerAddress("kafka.broker.addr")
        .build();

    assertNotNull(options);
    assertEquals(options.getTopicName(), "store_v1");
    assertTrue(options.getKeySerializer() instanceof KafkaKeySerializer);
    assertEquals(options.getValueSerializer(), valSer);
    assertEquals(options.getWriteComputeSerializer(), wcSer);
    assertEquals(options.getPartitioner(), venicePartitioner);
    assertEquals(options.getTime(), time);
    assertEquals((int) options.getPartitionCount(), 20);
    assertTrue(options.isChunkingEnabled());
    assertTrue(options.isRmdChunkingEnabled());
    assertEquals(options.getBrokerAddress(), "kafka.broker.addr");
  }

  @Test
  public void testVeniceWriterOptionsCanSetKeySer() {
    VeniceAvroKafkaSerializer keySer = new VeniceAvroKafkaSerializer("\"string\"");
    VeniceWriterOptions options = new VeniceWriterOptions.Builder("store_v1").setKeySerializer(keySer).build();
    assertNotNull(options);
    assertEquals(options.getKeySerializer(), keySer);
  }
}
