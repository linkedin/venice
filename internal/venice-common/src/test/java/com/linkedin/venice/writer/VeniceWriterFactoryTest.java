package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterConcurrentDelegator;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterDelegator;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class VeniceWriterFactoryTest {
  @Test
  public void testVeniceWriterFactory() {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMock = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<String> brokerAddrCapture = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<VeniceProperties> propertiesCapture = ArgumentCaptor.forClass(VeniceProperties.class);
    when(producerFactoryMock.create(propertiesCapture.capture(), eq("store_v1"), brokerAddrCapture.capture()))
        .thenReturn(producerAdapterMock);

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(new Properties(), producerFactoryMock, null);
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898").setPartitionCount(1).build())) {
      when(producerAdapterMock.getBrokerAddress()).thenReturn(brokerAddrCapture.getValue());
      assertNotNull(veniceWriter);

      String capturedBrokerAddr = veniceWriter.getDestination();
      assertNotNull(capturedBrokerAddr);
      assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");

      assertEquals(veniceWriter.getMaxRecordSizeBytes(), VeniceWriter.UNLIMITED_MAX_RECORD_SIZE);

      VeniceProperties capturedProperties = propertiesCapture.getValue();
      assertNotNull(capturedProperties);
      assertFalse(capturedProperties.containsKey(ApacheKafkaProducerConfig.KAFKA_COMPRESSION_TYPE));
    }
  }

  @Test
  public void testVeniceWriterFactoryWithProducerCompressionDisabled() {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMock = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<String> brokerAddrCapture = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<VeniceProperties> propertiesCapture = ArgumentCaptor.forClass(VeniceProperties.class);
    when(producerFactoryMock.create(propertiesCapture.capture(), eq("store_v1"), brokerAddrCapture.capture()))
        .thenReturn(producerAdapterMock);

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(new Properties(), producerFactoryMock, null);
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898")
            .setPartitionCount(1)
            .setProducerCompressionEnabled(false)
            .setProducerCount(5)
            .build())) {
      when(producerAdapterMock.getBrokerAddress()).thenReturn(brokerAddrCapture.getValue());
      assertNotNull(veniceWriter);

      String capturedBrokerAddr = veniceWriter.getDestination();
      assertNotNull(capturedBrokerAddr);
      assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");

      assertEquals(veniceWriter.getMaxRecordSizeBytes(), VeniceWriter.UNLIMITED_MAX_RECORD_SIZE);

      VeniceProperties capturedProperties = propertiesCapture.getValue();
      assertNotNull(capturedProperties);
      assertFalse(capturedProperties.getBoolean(ApacheKafkaProducerConfig.KAFKA_COMPRESSION_TYPE));

      verify(producerFactoryMock, times(5)).create(any(), any(), any());
      assertTrue(veniceWriter.getProducerAdapter() instanceof PubSubProducerAdapterDelegator);
    }

    // test concurrent delegator
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898")
            .setPartitionCount(1)
            .setProducerCompressionEnabled(false)
            .setProducerThreadCount(3)
            .build())) {
      when(producerAdapterMock.getBrokerAddress()).thenReturn(brokerAddrCapture.getValue());
      assertNotNull(veniceWriter);

      String capturedBrokerAddr = veniceWriter.getDestination();
      assertNotNull(capturedBrokerAddr);
      assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");

      assertEquals(veniceWriter.getMaxRecordSizeBytes(), VeniceWriter.UNLIMITED_MAX_RECORD_SIZE);

      VeniceProperties capturedProperties = propertiesCapture.getValue();
      assertNotNull(capturedProperties);
      assertFalse(capturedProperties.getBoolean(ApacheKafkaProducerConfig.KAFKA_COMPRESSION_TYPE));

      verify(producerFactoryMock, times(8)).create(any(), any(), any());
      assertTrue(veniceWriter.getProducerAdapter() instanceof PubSubProducerAdapterConcurrentDelegator);
    }
  }

  @Test
  public void testVeniceWriterFactoryCreatesProducerAdapterFactory() {
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(new Properties(), null, null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());

    veniceWriterFactory = new VeniceWriterFactory(new Properties(), null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);

    veniceWriterFactory = new VeniceWriterFactory(new Properties());
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);

    veniceWriterFactory = new VeniceWriterFactory(new Properties(), new ApacheKafkaProducerAdapterFactory(), null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);
  }
}
