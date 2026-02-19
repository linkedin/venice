package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.spark.sql.connector.read.InputPartition;
import org.testng.annotations.Test;


public class SparkPubSubPartitionReaderFactoryTest {
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateReaderWithInvalidPartitionType() {
    Properties p = new Properties();
    p.setProperty(KAFKA_INPUT_BROKER_URL, "localhost:9092");
    p.setProperty(KAFKA_INPUT_TOPIC, "test-topic");
    VeniceProperties config = new VeniceProperties(p);

    SparkPubSubPartitionReaderFactory factory = new SparkPubSubPartitionReaderFactory(config);

    InputPartition invalidPartition = new InputPartition() {
    };
    factory.createReader(invalidPartition);
  }
}
