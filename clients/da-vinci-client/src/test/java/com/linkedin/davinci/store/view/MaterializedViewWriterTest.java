package com.linkedin.davinci.store.view;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewParameters;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.time.Clock;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MaterializedViewWriterTest {
  private static final Schema SCHEMA = AvroCompatibilityHelper.parse("\"string\"");

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
    ViewParameters.Builder viewParamsBuilder = new ViewParameters.Builder(viewName);
    viewParamsBuilder.setPartitionCount(6);
    viewParamsBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    Map<String, String> viewParamsMap = viewParamsBuilder.build();
    VeniceConfigLoader props = getMockProps();
    MaterializedViewWriter materializedViewWriter = new MaterializedViewWriter(props, store, 1, SCHEMA, viewParamsMap);
    VeniceWriterOptions writerOptions = materializedViewWriter.buildWriterOptions(1);
    Assert.assertEquals(
        writerOptions.getTopicName(),
        Version.composeKafkaTopic(storeName, 1) + VeniceView.VIEW_TOPIC_SEPARATOR + viewName
            + MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX);
    Assert.assertEquals(writerOptions.getPartitionCount(), Integer.valueOf(6));
    Assert.assertEquals(writerOptions.getPartitioner().getClass(), DefaultVenicePartitioner.class);
    Assert.assertEquals(writerOptions.getProducerCount(), 3);
    Assert.assertTrue(writerOptions.isProducerCompressionEnabled());
  }

  @Test
  public void testProcessIngestionHeartbeat() {
    String storeName = "testStore";
    String viewName = "testMaterializedView";
    Version version = mock(Version.class);
    doReturn(true).when(version).isChunkingEnabled();
    doReturn(true).when(version).isRmdChunkingEnabled();
    Store store = getMockStore(storeName, 1, version);
    ViewParameters.Builder viewParamsBuilder = new ViewParameters.Builder(viewName);
    viewParamsBuilder.setPartitionCount(6);
    viewParamsBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    Map<String, String> viewParamsMap = viewParamsBuilder.build();
    VeniceConfigLoader props = getMockProps();
    Clock clock = mock(Clock.class);
    long startTime = System.currentTimeMillis();
    doReturn(startTime).when(clock).millis();
    MaterializedViewWriter materializedViewWriter =
        new MaterializedViewWriter(props, store, 1, SCHEMA, viewParamsMap, clock);
    materializedViewWriter.buildWriterOptions(1);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(KafkaKey.HEART_BEAT.getKey()).when(kafkaKey).getKey();
    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    when(veniceWriter.sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));
    doReturn(CompletableFuture.completedFuture(null)).when(veniceWriter)
        .sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong());
    materializedViewWriter.setVeniceWriter(veniceWriter);
    KafkaMessageEnvelope kafkaMessageEnvelope = mock(KafkaMessageEnvelope.class);
    ProducerMetadata producerMetadata = mock(ProducerMetadata.class);
    doReturn(producerMetadata).when(kafkaMessageEnvelope).getProducerMetadata();
    doReturn(startTime).when(producerMetadata).getMessageTimestamp();
    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    doReturn(true).when(partitionConsumptionState).isCompletionReported();

    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 1, partitionConsumptionState);
    verify(veniceWriter, never()).sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong());
    long newTime = startTime + TimeUnit.MINUTES.toMillis(4);
    doReturn(newTime).when(clock).millis();
    doReturn(startTime + TimeUnit.MINUTES.toMillis(1)).when(producerMetadata).getMessageTimestamp();
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 1, partitionConsumptionState);
    // We still don't expect any broadcast from partition 1 leader because staleness is within 5 minutes
    verify(veniceWriter, never()).sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), anyLong());
    doReturn(newTime).when(producerMetadata).getMessageTimestamp();
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 0, partitionConsumptionState);
    // Partition 0's leader should broadcast based on last broadcast timestamp (0) regardless of staleness threshold
    ArgumentCaptor<Long> heartbeatTimestampCaptor = ArgumentCaptor.forClass(Long.class);
    verify(veniceWriter, times(6))
        .sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), heartbeatTimestampCaptor.capture());
    // The low watermark for this materialized view writer should be the latest heartbeat stamp received by partition 0
    // since the low watermark from partition 1 was ignored due to DEFAULT_HEARTBEAT_BROADCAST_DELAY_THRESHOLD
    for (Long timestamp: heartbeatTimestampCaptor.getAllValues()) {
      Assert.assertEquals(timestamp, Long.valueOf(newTime));
    }
    newTime = newTime + TimeUnit.SECONDS.toMillis(30);
    doReturn(newTime).when(clock).millis();
    doReturn(startTime).when(producerMetadata).getMessageTimestamp();
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 2, partitionConsumptionState);
    doReturn(newTime).when(producerMetadata).getMessageTimestamp();
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 1, partitionConsumptionState);
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 0, partitionConsumptionState);
    // No new broadcast since it's still within DEFAULT_HEARTBEAT_BROADCAST_INTERVAL_MS (1 minute) since last broadcast.
    verify(veniceWriter, times(6))
        .sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), heartbeatTimestampCaptor.capture());
    newTime = newTime + TimeUnit.MINUTES.toMillis(3);
    doReturn(newTime).when(clock).millis();
    doReturn(newTime).when(producerMetadata).getMessageTimestamp();
    materializedViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 1, partitionConsumptionState);
    // We should broadcast the stale heartbeat timestamp from partition 2 since it's > than the reporting threshold
    verify(veniceWriter, times(12))
        .sendHeartbeat(anyString(), anyInt(), any(), any(), anyBoolean(), any(), heartbeatTimestampCaptor.capture());
    Assert.assertEquals(heartbeatTimestampCaptor.getValue(), Long.valueOf(startTime));
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
    return store;
  }
}
