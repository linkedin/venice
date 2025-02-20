package com.linkedin.davinci.store.view;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.utils.UnitTestComplexPartitioner;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MaterializedViewWriterTest {
  private static final Schema SCHEMA = AvroCompatibilityHelper.parse("\"string\"");
  private static final Random RANDOM = new Random(123);

  @Test
  public void testViewParametersBuilder() throws JsonProcessingException {
    String viewName = "testMaterializedView";
    int partitionCount = 3;
    MaterializedViewParameters.Builder viewParamsBuilder = new MaterializedViewParameters.Builder(viewName);
    Map<String, String> viewParams = viewParamsBuilder.build();
    Assert.assertEquals(viewParams.size(), 1);
    Assert.assertEquals(viewParams.get(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name()), viewName);
    viewParamsBuilder.setPartitionCount(partitionCount);
    List<String> projectionFields = Arrays.asList("field1", "field2");
    viewParamsBuilder.setProjectionFields(projectionFields);
    viewParams = viewParamsBuilder.build();
    Assert.assertEquals(viewParams.size(), 3);
    Assert.assertEquals(
        viewParams.get(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name()),
        String.valueOf(partitionCount));
    Assert.assertEquals(
        viewParams.get(MaterializedViewParameters.MATERIALIZED_VIEW_PROJECTION_FIELDS.name()),
        ObjectMapperFactory.getInstance().writeValueAsString(projectionFields));
  }

  @Test
  public void testBuildWriterOptions() {
    String storeName = "testStore";
    String viewName = "testMaterializedView";
    Version version = mock(Version.class);
    doReturn(true).when(version).isChunkingEnabled();
    doReturn(true).when(version).isRmdChunkingEnabled();
    Store store = getMockStore(storeName, 1, version);
    doReturn(true).when(store).isNearlineProducerCompressionEnabled();
    doReturn(3).when(store).getNearlineProducerCountPerWriter();
    MaterializedViewParameters.Builder viewParamsBuilder = new MaterializedViewParameters.Builder(viewName);
    viewParamsBuilder.setPartitionCount(6);
    viewParamsBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    Map<String, String> viewParamsMap = viewParamsBuilder.build();
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), viewParamsMap);
    doReturn(Collections.singletonMap(viewName, viewConfig)).when(version).getViewConfigs();
    VeniceConfigLoader props = getMockProps();
    VeniceViewWriterFactory viewWriterFactory = new VeniceViewWriterFactory(props);
    VeniceViewWriter viewWriter = viewWriterFactory.buildStoreViewWriters(store, 1, SCHEMA).get(viewName);
    Assert.assertTrue(viewWriter instanceof MaterializedViewWriter);
    MaterializedViewWriter materializedViewWriter = (MaterializedViewWriter) viewWriter;
    VeniceWriterOptions writerOptions = materializedViewWriter.buildWriterOptions();
    Assert.assertEquals(
        writerOptions.getTopicName(),
        Version.composeKafkaTopic(storeName, 1) + VeniceView.VIEW_NAME_SEPARATOR + viewName
            + MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX);
    Assert.assertEquals(writerOptions.getPartitionCount(), Integer.valueOf(6));
    Assert.assertEquals(writerOptions.getPartitioner().getClass(), DefaultVenicePartitioner.class);
    Assert.assertEquals(writerOptions.getProducerCount(), 3);
    Assert.assertTrue(writerOptions.isProducerCompressionEnabled());
  }

  @Test
  public void testProcessControlMessage() {
    String storeName = "testStore";
    String viewName = "testMaterializedView";
    Version version = mock(Version.class);
    doReturn(true).when(version).isChunkingEnabled();
    doReturn(true).when(version).isRmdChunkingEnabled();
    getMockStore(storeName, 1, version);
    MaterializedViewParameters.Builder viewParamsBuilder = new MaterializedViewParameters.Builder(viewName);
    viewParamsBuilder.setPartitionCount(6);
    viewParamsBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    Map<String, String> viewParamsMap = viewParamsBuilder.build();
    VeniceConfigLoader props = getMockProps();
    MaterializedViewWriter materializedViewWriter = new MaterializedViewWriter(props, version, SCHEMA, viewParamsMap);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(KafkaKey.HEART_BEAT.getKey()).when(kafkaKey).getKey();
    ComplexVeniceWriter veniceWriter = mock(ComplexVeniceWriter.class);
    when(veniceWriter.sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));
    doReturn(CompletableFuture.completedFuture(null)).when(veniceWriter)
        .sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong());
    materializedViewWriter.setVeniceWriter(veniceWriter);
    KafkaMessageEnvelope kafkaMessageEnvelope = mock(KafkaMessageEnvelope.class);
    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 1, partitionConsumptionState);
    verify(veniceWriter, never()).sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong());
  }

  @Test
  public void testViewWriterCanForwardChunkedKeysCorrectly() {
    String storeName = "testStoreWithChunkedKeys";
    String viewName = "testMaterializedViewWithChunkedKeys";
    Version version = mock(Version.class);
    doReturn(true).when(version).isChunkingEnabled();
    doReturn(true).when(version).isRmdChunkingEnabled();
    getMockStore(storeName, 1, version);
    MaterializedViewParameters.Builder viewParamsBuilder = new MaterializedViewParameters.Builder(viewName);
    viewParamsBuilder.setPartitionCount(6);
    viewParamsBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    Map<String, String> viewParamsMap = viewParamsBuilder.build();
    VeniceConfigLoader props = getMockProps();
    MaterializedViewWriter materializedViewWriter = new MaterializedViewWriter(props, version, SCHEMA, viewParamsMap);
    ComplexVeniceWriter veniceWriter = mock(ComplexVeniceWriter.class);
    doReturn(CompletableFuture.completedFuture(null)).when(veniceWriter).complexPut(any(), any(), anyInt(), any());
    materializedViewWriter.setVeniceWriter(veniceWriter);
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    ByteBuffer dummyValue = mock(ByteBuffer.class);
    // Deterministic random bytes
    int keySize = 5;
    for (int i = 0; i < 100; i++) {
      byte[] key = new byte[keySize];
      RANDOM.nextBytes(key);
      materializedViewWriter.processRecord(
          dummyValue,
          keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key),
          1,
          true,
          Lazy.of(() -> null));
      verify(veniceWriter, times(1)).complexPut(eq(key), any(), eq(1), any());
      Mockito.clearInvocations(veniceWriter);
    }
  }

  @Test
  public void testIsComplexVenicePartitioner() {
    String storeName = "testStore";
    String viewName = "simplePartitionerView";
    Version version = mock(Version.class);
    doReturn(true).when(version).isChunkingEnabled();
    doReturn(true).when(version).isRmdChunkingEnabled();
    getMockStore(storeName, 1, version);
    MaterializedViewParameters.Builder viewParamsBuilder = new MaterializedViewParameters.Builder(viewName);
    viewParamsBuilder.setPartitionCount(6);
    viewParamsBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    VeniceConfigLoader props = getMockProps();
    MaterializedViewWriter materializedViewWriter =
        new MaterializedViewWriter(props, version, SCHEMA, viewParamsBuilder.build());
    Assert.assertFalse(materializedViewWriter.isComplexVenicePartitioner());
    String complexViewName = "complexPartitionerView";
    MaterializedViewParameters.Builder complexViewParamsBuilder =
        new MaterializedViewParameters.Builder(complexViewName);
    complexViewParamsBuilder.setPartitionCount(6);
    complexViewParamsBuilder.setPartitioner(UnitTestComplexPartitioner.class.getCanonicalName());
    MaterializedViewWriter complexMaterializedViewWriter =
        new MaterializedViewWriter(props, version, SCHEMA, complexViewParamsBuilder.build());
    Assert.assertTrue(complexMaterializedViewWriter.isComplexVenicePartitioner());
  }

  private VeniceConfigLoader getMockProps() {
    VeniceConfigLoader props = mock(VeniceConfigLoader.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    PubSubClientsFactory pubSubClientsFactory = mock(PubSubClientsFactory.class);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory = mock(PubSubProducerAdapterFactory.class);
    doReturn(pubSubProducerAdapterFactory).when(pubSubClientsFactory).getProducerAdapterFactory();
    doReturn(pubSubClientsFactory).when(serverConfig).getPubSubClientsFactory();
    doReturn(serverConfig).when(props).getVeniceServerConfig();
    VeniceProperties veniceProperties = new VeniceProperties(new Properties());
    doReturn(veniceProperties).when(props).getCombinedProperties();
    return props;
  }

  private Store getMockStore(String storeName, int versionNumber, Version version) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    doReturn(version).when(store).getVersionOrThrow(versionNumber);
    doReturn(storeName).when(version).getStoreName();
    doReturn(versionNumber).when(version).getNumber();
    return store;
  }
}
