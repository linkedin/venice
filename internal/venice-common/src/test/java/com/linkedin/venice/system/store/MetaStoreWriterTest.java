package com.linkedin.venice.system.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetaStoreWriterTest {
  @Test
  public void testMetaStoreWriterWillRestartUponProduceFailure() {
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    String metaStoreName = "testStore";
    ReentrantLock reentrantLock = new ReentrantLock();
    when(metaStoreWriter.getOrCreateMetaStoreWriterLock(metaStoreName)).thenReturn(reentrantLock);
    VeniceWriter badWriter = mock(VeniceWriter.class);
    when(badWriter.delete(any(), any())).thenThrow(new VeniceException("Bad producer call"));
    VeniceWriter goodWriter = mock(VeniceWriter.class);
    when(metaStoreWriter.getOrCreateMetaStoreWriter(metaStoreName)).thenReturn(badWriter, goodWriter);
    doCallRealMethod().when(metaStoreWriter).removeMetaStoreWriter(anyString());
    doCallRealMethod().when(metaStoreWriter).writeMessageWithRetry(anyString(), any());
    metaStoreWriter.writeMessageWithRetry(metaStoreName, vw -> vw.delete("a", null));
    verify(badWriter, times(1)).delete(any(), any());
    verify(goodWriter, times(1)).delete(any(), any());
  }

  @Test
  public void testMetaStoreWriterSendHeartbeatMessage() {
    // Mock
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    ReentrantLock reentrantLock = new ReentrantLock();
    String metaStoreName = "testStore";
    when(metaStoreWriter.getOrCreateMetaStoreWriterLock(anyString())).thenReturn(reentrantLock);
    VeniceWriter goodWriter = mock(VeniceWriter.class);
    when(metaStoreWriter.getOrCreateMetaStoreWriter(anyString())).thenReturn(goodWriter);
    doCallRealMethod().when(metaStoreWriter).writeHeartbeat(anyString(), anyLong());
    doCallRealMethod().when(metaStoreWriter).writeMessageWithRetry(anyString(), any());
    long timestamp = 123L;

    // Action
    metaStoreWriter.writeHeartbeat(metaStoreName, timestamp);
    ArgumentCaptor<StoreMetaKey> keyArgumentCaptor = ArgumentCaptor.forClass(StoreMetaKey.class);
    ArgumentCaptor<StoreMetaValue> valueArgumentCaptor = ArgumentCaptor.forClass(StoreMetaValue.class);
    ArgumentCaptor<Integer> schemaArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(goodWriter).put(keyArgumentCaptor.capture(), valueArgumentCaptor.capture(), schemaArgumentCaptor.capture());

    // Assertion
    StoreMetaKey capturedKey = keyArgumentCaptor.getValue();
    Assert.assertEquals(capturedKey.keyStrings, Collections.singletonList(metaStoreName));
    Assert.assertEquals(capturedKey.metadataType, MetaStoreDataType.HEARTBEAT.getValue());
    StoreMetaValue capturedValue = valueArgumentCaptor.getValue();
    Assert.assertEquals(capturedValue.timestamp, timestamp);
  }
}
