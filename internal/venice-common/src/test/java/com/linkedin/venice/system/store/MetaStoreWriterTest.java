package com.linkedin.venice.system.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.concurrent.locks.ReentrantLock;
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
}
