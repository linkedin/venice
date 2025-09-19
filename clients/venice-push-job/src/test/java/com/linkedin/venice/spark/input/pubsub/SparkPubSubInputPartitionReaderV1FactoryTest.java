package com.linkedin.venice.spark.input.pubsub;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.spark.sql.connector.read.InputPartition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SparkPubSubInputPartitionReaderV1FactoryTest {
  @Test
  public void testCreateReaderWithNonMatchingInputPartitionType() {
    // Arrange
    VeniceProperties jobConfig = new VeniceProperties(new Properties());
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
    VeniceProperties jobConfig = new VeniceProperties(new Properties());
    SparkPubSubPartitionReaderFactory factory = new SparkPubSubPartitionReaderFactory(jobConfig);
    InputPartition mockInputPartition = Mockito.mock(InputPartition.class);

    // Act & Assert
    Assert.assertFalse(factory.supportColumnarReads(mockInputPartition), "Factory should not support columnar reads");
  }
}
