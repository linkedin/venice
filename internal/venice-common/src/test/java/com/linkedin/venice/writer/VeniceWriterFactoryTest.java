package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterConcurrentDelegator;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
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
    ArgumentCaptor<PubSubProducerAdapterContext> producerCtxCaptor =
        ArgumentCaptor.forClass(PubSubProducerAdapterContext.class);

    when(producerFactoryMock.create(producerCtxCaptor.capture())).thenReturn(producerAdapterMock);
    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(properties, producerFactoryMock, null);
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898").setPartitionCount(1).build())) {
      PubSubProducerAdapterContext capturedProducerCtx = producerCtxCaptor.getValue();
      when(producerAdapterMock.getBrokerAddress()).thenReturn(capturedProducerCtx.getBrokerAddress());
      assertNotNull(veniceWriter);
      String capturedBrokerAddr = veniceWriter.getDestination();
      assertNotNull(capturedBrokerAddr);
      assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");
      assertEquals(veniceWriter.getMaxRecordSizeBytes(), VeniceWriter.UNLIMITED_MAX_RECORD_SIZE);
      VeniceProperties capturedProperties = capturedProducerCtx.getVeniceProperties();
      assertNotNull(capturedProperties);
    }
  }

  @Test
  public void testVeniceWriterFactoryWithProducerCompressionDisabled() {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMock = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<PubSubProducerAdapterContext> producerCtxCaptor =
        ArgumentCaptor.forClass(PubSubProducerAdapterContext.class);
    when(producerFactoryMock.create(producerCtxCaptor.capture())).thenReturn(producerAdapterMock);

    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(properties, producerFactoryMock, null);
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898")
            .setPartitionCount(1)
            .setProducerCompressionEnabled(false)
            .setProducerCount(5)
            .build())) {

      PubSubProducerAdapterContext capturedProducerCtx = producerCtxCaptor.getValue();
      when(producerAdapterMock.getBrokerAddress()).thenReturn(capturedProducerCtx.getBrokerAddress());
      assertNotNull(veniceWriter);
      String capturedBrokerAddr = veniceWriter.getDestination();
      assertNotNull(capturedBrokerAddr);
      assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");

      assertEquals(veniceWriter.getMaxRecordSizeBytes(), VeniceWriter.UNLIMITED_MAX_RECORD_SIZE);
      VeniceProperties capturedProperties = capturedProducerCtx.getVeniceProperties();
      assertNotNull(capturedProperties);
      assertFalse(capturedProducerCtx.isProducerCompressionEnabled());

      verify(producerFactoryMock, times(5)).create(any(PubSubProducerAdapterContext.class));
      assertTrue(veniceWriter.getProducerAdapter() instanceof PubSubProducerAdapterDelegator);
    }

    // test concurrent delegator
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898")
            .setPartitionCount(1)
            .setProducerCompressionEnabled(false)
            .setProducerThreadCount(3)
            .build())) {
      PubSubProducerAdapterContext capturedProducerCtx = producerCtxCaptor.getValue();
      when(producerAdapterMock.getBrokerAddress()).thenReturn(capturedProducerCtx.getBrokerAddress());
      assertNotNull(veniceWriter);

      String capturedBrokerAddr = veniceWriter.getDestination();
      assertNotNull(capturedBrokerAddr);
      assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");

      assertEquals(veniceWriter.getMaxRecordSizeBytes(), VeniceWriter.UNLIMITED_MAX_RECORD_SIZE);
      assertFalse(capturedProducerCtx.isProducerCompressionEnabled());

      verify(producerFactoryMock, times(8)).create(any(PubSubProducerAdapterContext.class));
      assertTrue(veniceWriter.getProducerAdapter() instanceof PubSubProducerAdapterConcurrentDelegator);
    }
  }

  @Test
  public void testVeniceWriterFactoryCreatesProducerAdapterFactory() {
    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(properties, null, null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());

    veniceWriterFactory = new VeniceWriterFactory(properties);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);

    veniceWriterFactory = new VeniceWriterFactory(properties, new ApacheKafkaProducerAdapterFactory(), null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);
  }
}
