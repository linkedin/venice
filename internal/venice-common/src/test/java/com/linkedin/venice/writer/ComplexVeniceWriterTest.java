package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.partitioner.ComplexVenicePartitioner;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComplexVeniceWriterTest {
  private static final String PARTITION_FIELD_NAME = "partition";
  private static final byte[] IGNORED_BYTES = new byte[0];
  private static final String DEFAULT_VIEW_TOPIC_NAME = "testStore_v1_MaterializedViewTest_mv";

  private static class ValuePartitionerForUT extends ComplexVenicePartitioner {
    public ValuePartitionerForUT() {
      super();
    }

    /**
     * Return partition array based on provided PARTITION_FIELD_NAME value:
     *   - null : []
     *   - A : [1]
     *   - B : [1, 2]
     *   - anything else : []
     */
    @Override
    public int[] getPartitionId(byte[] keyBytes, GenericRecord value, int numPartitions) {
      Object partitionField = value.get(PARTITION_FIELD_NAME);
      if (partitionField == null) {
        return new int[0];
      }
      String fieldValueString = (String) partitionField;
      int[] partitions;
      if ("A".equals(fieldValueString)) {
        partitions = new int[1];
        partitions[0] = 1;
      } else if ("B".equals(fieldValueString)) {
        partitions = new int[2];
        partitions[0] = 1;
        partitions[1] = 2;
      } else {
        partitions = new int[0];
      }
      return partitions;
    }

    @Override
    public int getPartitionId(byte[] keyBytes, int numPartitions) {
      return 0;
    }

    @Override
    public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
      return 0;
    }
  }

  @Test
  public void testUnsupportedPublicAPIs() {
    PubSubProducerAdapter mockProducerAdapter = mock(PubSubProducerAdapter.class);
    VeniceProperties veniceProperties = new VeniceProperties(new Properties());
    ComplexVeniceWriter<byte[], byte[], byte[]> complexVeniceWriter = new ComplexVeniceWriter<>(
        getVeniceWriterOptions(new DefaultVenicePartitioner(), 1),
        veniceProperties,
        mockProducerAdapter);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> complexVeniceWriter.put(IGNORED_BYTES, IGNORED_BYTES, 1, null));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> complexVeniceWriter.update(IGNORED_BYTES, IGNORED_BYTES, 1, 1, null));
    Assert.assertThrows(UnsupportedOperationException.class, () -> complexVeniceWriter.delete(IGNORED_BYTES, null));
  }

  @Test
  public void testComplexPut() throws ExecutionException, InterruptedException {
    PubSubProducerAdapter mockProducerAdapter = mock(PubSubProducerAdapter.class);
    doReturn(CompletableFuture.completedFuture(null)).when(mockProducerAdapter)
        .sendMessage(anyString(), anyInt(), any(), any(), any(), any());
    VeniceProperties veniceProperties = new VeniceProperties(new Properties());
    // Simple partitioner
    ComplexVeniceWriter<byte[], byte[], byte[]> simplePartitionerWriter = new ComplexVeniceWriter<>(
        getVeniceWriterOptions(new DefaultVenicePartitioner(), 1),
        veniceProperties,
        mockProducerAdapter);
    Assert.assertThrows(
        VeniceException.class,
        () -> simplePartitionerWriter.complexPut(IGNORED_BYTES, null, 1, Lazy.of(() -> null)));
    simplePartitionerWriter.complexPut(IGNORED_BYTES, IGNORED_BYTES, 1, Lazy.of(() -> null)).get();
    verify(mockProducerAdapter, atLeastOnce()).sendMessage(anyString(), eq(0), any(), any(), any(), any());
    // Complex partitioner
    PubSubProducerAdapter mockProducerAdapterForComplexWrites = mock(PubSubProducerAdapter.class);
    doReturn(CompletableFuture.completedFuture(null)).when(mockProducerAdapterForComplexWrites)
        .sendMessage(anyString(), anyInt(), any(), any(), any(), any());
    ComplexVeniceWriter<byte[], byte[], byte[]> complexPartitionerWriter = new ComplexVeniceWriter<>(
        getVeniceWriterOptions(new ValuePartitionerForUT(), 3),
        veniceProperties,
        mockProducerAdapterForComplexWrites);
    GenericRecord mockRecord = mock(GenericRecord.class);
    complexPartitionerWriter.complexPut(IGNORED_BYTES, IGNORED_BYTES, 1, Lazy.of(() -> mockRecord)).get();
    doReturn("Foo").when(mockRecord).get(PARTITION_FIELD_NAME);
    complexPartitionerWriter.complexPut(IGNORED_BYTES, IGNORED_BYTES, 1, Lazy.of(() -> mockRecord)).get();
    verify(mockProducerAdapterForComplexWrites, never()).sendMessage(anyString(), anyInt(), any(), any(), any(), any());
    doReturn("A").when(mockRecord).get(PARTITION_FIELD_NAME);
    complexPartitionerWriter.complexPut(IGNORED_BYTES, IGNORED_BYTES, 1, Lazy.of(() -> mockRecord)).get();
    verify(mockProducerAdapterForComplexWrites, atLeastOnce())
        .sendMessage(anyString(), eq(1), any(), any(), any(), any());
    verify(mockProducerAdapterForComplexWrites, never()).sendMessage(anyString(), eq(0), any(), any(), any(), any());
    verify(mockProducerAdapterForComplexWrites, never()).sendMessage(anyString(), eq(2), any(), any(), any(), any());
    Mockito.clearInvocations(mockProducerAdapterForComplexWrites);
    doReturn("B").when(mockRecord).get(PARTITION_FIELD_NAME);
    complexPartitionerWriter.complexPut(IGNORED_BYTES, IGNORED_BYTES, 1, Lazy.of(() -> mockRecord)).get();
    verify(mockProducerAdapterForComplexWrites, atLeastOnce())
        .sendMessage(anyString(), eq(1), any(), any(), any(), any());
    verify(mockProducerAdapterForComplexWrites, atLeastOnce())
        .sendMessage(anyString(), eq(2), any(), any(), any(), any());
    verify(mockProducerAdapterForComplexWrites, never()).sendMessage(anyString(), eq(0), any(), any(), any(), any());
  }

  @Test
  public void testComplexDelete() throws ExecutionException, InterruptedException {
    PubSubProducerAdapter mockProducerAdapter = mock(PubSubProducerAdapter.class);
    doReturn(CompletableFuture.completedFuture(null)).when(mockProducerAdapter)
        .sendMessage(anyString(), anyInt(), any(), any(), any(), any());
    VeniceProperties veniceProperties = new VeniceProperties(new Properties());
    // Simple partitioner
    ComplexVeniceWriter<byte[], byte[], byte[]> simplePartitionerWriter = new ComplexVeniceWriter<>(
        getVeniceWriterOptions(new DefaultVenicePartitioner(), 1),
        veniceProperties,
        mockProducerAdapter);
    simplePartitionerWriter.complexDelete(IGNORED_BYTES, Lazy.of(() -> null)).get();
    verify(mockProducerAdapter, atLeastOnce()).sendMessage(anyString(), eq(0), any(), any(), any(), any());
    // Complex partitioner
    PubSubProducerAdapter mockProducerAdapterForComplexWrites = mock(PubSubProducerAdapter.class);
    doReturn(CompletableFuture.completedFuture(null)).when(mockProducerAdapterForComplexWrites)
        .sendMessage(anyString(), anyInt(), any(), any(), any(), any());
    ComplexVeniceWriter<byte[], byte[], byte[]> complexPartitionerWriter = new ComplexVeniceWriter<>(
        getVeniceWriterOptions(new ValuePartitionerForUT(), 3),
        veniceProperties,
        mockProducerAdapterForComplexWrites);
    // Null value should be ignored
    complexPartitionerWriter.complexDelete(IGNORED_BYTES, Lazy.of(() -> null)).get();
    verify(mockProducerAdapterForComplexWrites, never()).sendMessage(anyString(), anyInt(), any(), any(), any(), any());
    GenericRecord mockRecord = mock(GenericRecord.class);
    doReturn("B").when(mockRecord).get(PARTITION_FIELD_NAME);
    complexPartitionerWriter.complexDelete(IGNORED_BYTES, Lazy.of(() -> mockRecord)).get();
    verify(mockProducerAdapterForComplexWrites, atLeastOnce())
        .sendMessage(anyString(), eq(1), any(), any(), any(), any());
    verify(mockProducerAdapterForComplexWrites, atLeastOnce())
        .sendMessage(anyString(), eq(2), any(), any(), any(), any());
    verify(mockProducerAdapterForComplexWrites, never()).sendMessage(anyString(), eq(0), any(), any(), any(), any());
  }

  private VeniceWriterOptions getVeniceWriterOptions(VenicePartitioner partitioner, int partitionCount) {
    VeniceWriterOptions.Builder configBuilder = new VeniceWriterOptions.Builder(DEFAULT_VIEW_TOPIC_NAME);
    configBuilder.setPartitionCount(partitionCount).setPartitioner(partitioner);
    return configBuilder.build();
  }
}
