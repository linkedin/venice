package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.spark.sql.connector.read.InputPartition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SparkPubSubInputPartitionReaderV1FactoryTest {
  private static VeniceProperties createMinimalConfig() {
    Properties props = new Properties();
    props.setProperty(KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED, "false");
    return new VeniceProperties(props);
  }

  @Test
  public void testCreateReaderWithNonMatchingInputPartitionType() {
    // Arrange
    VeniceProperties jobConfig = createMinimalConfig();
    SparkPubSubPartitionReaderFactory factory = new SparkPubSubPartitionReaderFactory(jobConfig);

    // Create a mock that does not implement VeniceBasicPubsubInputPartition
    InputPartition mockInputPartition = Mockito.mock(InputPartition.class);

    // Act & Assert
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class, () -> factory.createReader(mockInputPartition));

    // Verify exception message
    String expectedMessage = "SparkPubSubPartitionReaderFactory can only create readers for";
    String actualMessage = exception.getMessage();
    Assert.assertTrue(actualMessage.contains(expectedMessage), "Exception message should contain expected message");
  }

  @Test
  public void testSupportColumnarReads() {
    // Arrange
    VeniceProperties jobConfig = createMinimalConfig();
    SparkPubSubPartitionReaderFactory factory = new SparkPubSubPartitionReaderFactory(jobConfig);
    InputPartition mockInputPartition = Mockito.mock(InputPartition.class);

    // Act & Assert
    Assert.assertFalse(factory.supportColumnarReads(mockInputPartition), "Factory should not support columnar reads");
  }
}
