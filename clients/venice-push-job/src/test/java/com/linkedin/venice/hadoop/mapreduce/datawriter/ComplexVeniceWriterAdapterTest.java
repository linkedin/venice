package com.linkedin.venice.hadoop.mapreduce.datawriter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.hadoop.task.datawriter.ComplexVeniceWriterAdapter;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComplexVeniceWriterAdapterTest {
  private static final byte[] IGNORED_BYTES = new byte[0];

  @Test
  public void testUnsupportedOperations() throws ExecutionException, InterruptedException {
    ComplexVeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(ComplexVeniceWriter.class);
    GenericRecord mockRecord = mock(GenericRecord.class);
    ComplexVeniceWriterAdapter<byte[], byte[], byte[]> complexVeniceWriterAdapter = new ComplexVeniceWriterAdapter<>(
        "ignored",
        mockVeniceWriter,
        (ignoredId, ignoredBytes) -> mockRecord,
        (inputBytes) -> inputBytes);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> complexVeniceWriterAdapter.update(IGNORED_BYTES, IGNORED_BYTES, 1, 1, null));
    complexVeniceWriterAdapter.delete(IGNORED_BYTES, null, null).get();
    verify(mockVeniceWriter, never()).complexDelete(any(), any());
  }

  @Test
  public void testPut() throws ExecutionException, InterruptedException {
    ComplexVeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(ComplexVeniceWriter.class);
    GenericRecord mockRecord = mock(GenericRecord.class);
    doReturn(CompletableFuture.completedFuture(null)).when(mockVeniceWriter).complexPut(any(), any(), anyInt(), any());
    BiFunction<byte[], Integer, GenericRecord> deserializeFunction = (inputBytes, schemaId) -> {
      if (inputBytes.length >= 2) {
        return mockRecord;
      } else {
        return null;
      }
    };
    Function<byte[], byte[]> decompressFunction = (inputBytes) -> new byte[2];
    ComplexVeniceWriterAdapter<byte[], byte[], byte[]> complexVeniceWriterAdapter =
        new ComplexVeniceWriterAdapter<>("ignored", mockVeniceWriter, deserializeFunction, decompressFunction);
    byte[] valueBytes = new byte[1];
    complexVeniceWriterAdapter.put(IGNORED_BYTES, valueBytes, 1, null, null).get();
    ArgumentCaptor<Lazy<GenericRecord>> captor = ArgumentCaptor.forClass(Lazy.class);
    verify(mockVeniceWriter, times(1)).complexPut(eq(IGNORED_BYTES), eq(valueBytes), eq(1), captor.capture());
    // Verify the deserialize and decompress functions are invoked properly
    Assert.assertEquals(captor.getValue().get(), mockRecord);
  }
}
