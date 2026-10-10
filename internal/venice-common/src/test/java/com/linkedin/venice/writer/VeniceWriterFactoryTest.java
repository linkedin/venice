package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterConcurrentDelegator;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterDelegator;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(properties, producerFactoryMock, null, null);
    try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898").setPartitionCount(1).build())) {
      PubSubProducerAdapterContext capturedProducerCtx = producerCtxCaptor.getValue();
      assertNull(capturedProducerCtx.getPubSubEncryptionKeyUrnLookup());
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

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testWriterPropertiesDoNotChangeFactoryDefaults(boolean producerEncryptionEnabled) {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactory = mock(PubSubProducerAdapterFactory.class);
    ArgumentCaptor<PubSubProducerAdapterContext> contextCaptor =
        ArgumentCaptor.forClass(PubSubProducerAdapterContext.class);
    when(producerFactory.create(contextCaptor.capture())).thenAnswer(invocation -> mock(PubSubProducerAdapter.class));
    MetricsRepository metricsRepository = new MetricsRepository();
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
    Function<String, String> keyLookup = storeName -> "test-key";
    Properties defaults = new Properties();
    defaults.setProperty(ConfigKeys.PUBSUB_BROKER_ADDRESS, "default-broker:9092");
    defaults.setProperty("producer.custom.setting", "default");
    VeniceWriterFactory factory = new VeniceWriterFactory(
        defaults,
        producerFactory,
        metricsRepository,
        registry,
        keyLookup,
        producerEncryptionEnabled);
    Properties overrides = new Properties();
    overrides.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "cluster-broker:9092");
    overrides.setProperty("producer.custom.setting", "cluster");
    overrides.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, "1024");
    VeniceProperties writerProperties = new VeniceProperties(overrides);
    VeniceWriterOptions options = new VeniceWriterOptions.Builder("store_v1").setPartitionCount(1).build();

    try (VeniceWriter writer = factory.createVeniceWriter(options, writerProperties);
        VeniceWriter defaultWriter = factory.createVeniceWriter(options);
        VeniceWriter explicitBrokerWriter = factory.createVeniceWriter(
            new VeniceWriterOptions.Builder("store_v2").setPartitionCount(1)
                .setBrokerAddress("explicit-broker:9092")
                .build(),
            writerProperties)) {
      assertEquals(writer.getMaxSizeForUserPayloadPerMessageInBytes(), 1024);
      assertEquals(
          defaultWriter.getMaxSizeForUserPayloadPerMessageInBytes(),
          VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
      assertEquals(contextCaptor.getAllValues().get(0).getVeniceProperties(), writerProperties);
      assertEquals(contextCaptor.getAllValues().get(0).getBrokerAddress(), "cluster-broker:9092");
      assertEquals(contextCaptor.getAllValues().get(1).getBrokerAddress(), "default-broker:9092");
      assertEquals(
          contextCaptor.getAllValues().get(1).getVeniceProperties().getString("producer.custom.setting"),
          "default");
      assertEquals(contextCaptor.getAllValues().get(2).getBrokerAddress(), "explicit-broker:9092");
      for (PubSubProducerAdapterContext context: contextCaptor.getAllValues()) {
        assertSame(context.getMetricsRepository(), metricsRepository);
        assertSame(context.getPubSubPositionTypeRegistry(), registry);
        assertSame(context.getPubSubEncryptionKeyUrnLookup(), keyLookup);
        assertEquals(context.isProducerEncryptionEnabled(), producerEncryptionEnabled);
      }
    } finally {
      metricsRepository.close();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProducerEncryptionFlag(boolean producerEncryptionEnabled) {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMock = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<PubSubProducerAdapterContext> producerCtxCaptor =
        ArgumentCaptor.forClass(PubSubProducerAdapterContext.class);
    when(producerFactoryMock.create(producerCtxCaptor.capture())).thenReturn(producerAdapterMock);

    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");
    VeniceWriterFactory veniceWriterFactory =
        new VeniceWriterFactory(properties, producerFactoryMock, null, null, null, producerEncryptionEnabled);
    try (VeniceWriter ignored = veniceWriterFactory
        .createVeniceWriter(new VeniceWriterOptions.Builder("store_v1").setPartitionCount(1).build())) {
      assertEquals(producerCtxCaptor.getValue().isProducerEncryptionEnabled(), producerEncryptionEnabled);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProducerEncryptionFlagDerivedFromLookup(boolean lookupPresent) {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMock = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<PubSubProducerAdapterContext> producerCtxCaptor =
        ArgumentCaptor.forClass(PubSubProducerAdapterContext.class);
    when(producerFactoryMock.create(producerCtxCaptor.capture())).thenReturn(producerAdapterMock);

    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");
    Function<String, String> keyLookup = lookupPresent ? storeName -> "urn:test:key:1" : null;
    // 5-arg constructor must derive producerEncryptionEnabled from whether a lookup was supplied, not hardcode it.
    VeniceWriterFactory veniceWriterFactory =
        new VeniceWriterFactory(properties, producerFactoryMock, null, null, keyLookup);
    try (VeniceWriter ignored = veniceWriterFactory
        .createVeniceWriter(new VeniceWriterOptions.Builder("store_v1").setPartitionCount(1).build())) {
      assertEquals(producerCtxCaptor.getValue().isProducerEncryptionEnabled(), lookupPresent);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceWriterFactoryWithProducerCompressionDisabled(boolean lookupEnabled) {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMock = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<PubSubProducerAdapterContext> producerCtxCaptor =
        ArgumentCaptor.forClass(PubSubProducerAdapterContext.class);
    when(producerFactoryMock.create(producerCtxCaptor.capture())).thenReturn(producerAdapterMock);

    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");
    AtomicInteger lookups = new AtomicInteger();
    AtomicReference<String> keyUrn = new AtomicReference<>();
    Function<String, String> keyLookup = lookupEnabled ? storeName -> {
      lookups.incrementAndGet();
      return keyUrn.get();
    } : null;
    VeniceWriterFactory veniceWriterFactory =
        new VeniceWriterFactory(properties, producerFactoryMock, null, null, keyLookup);
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
    assertEquals(lookups.get(), 0);
    for (PubSubProducerAdapterContext context: producerCtxCaptor.getAllValues()) {
      assertSame(context.getPubSubEncryptionKeyUrnLookup(), keyLookup);
      if (lookupEnabled) {
        assertNull(context.getPubSubEncryptionKeyUrnLookup().apply("store"));
      }
    }
    if (lookupEnabled) {
      keyUrn.set("urn:test:key:1");
      for (PubSubProducerAdapterContext context: producerCtxCaptor.getAllValues()) {
        assertEquals(context.getPubSubEncryptionKeyUrnLookup().apply("store"), "urn:test:key:1");
      }
    }
  }

  @Test
  public void testVeniceWriterFactoryCreatesProducerAdapterFactory() {
    Properties properties = new Properties();
    properties.put(ConfigKeys.PUBSUB_BROKER_ADDRESS, "kafka:9898");

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(properties, null, null, null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());

    veniceWriterFactory = new VeniceWriterFactory(properties);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);

    veniceWriterFactory = new VeniceWriterFactory(properties, new ApacheKafkaProducerAdapterFactory(), null, null);
    assertNotNull(veniceWriterFactory.getProducerAdapterFactory());
    assertEquals(veniceWriterFactory.getProducerAdapterFactory().getClass(), ApacheKafkaProducerAdapterFactory.class);
  }
}
