package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.writer.VeniceWriterFactory;
import org.testng.annotations.Test;


public class StoreIngestionTaskFactoryTest {
  @Test
  public void testSelectsWriterFactoriesByStoreEncryption() {
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    VeniceWriterFactory encryptedWriterFactory = mock(VeniceWriterFactory.class);
    VeniceViewWriterFactory viewWriterFactory = mock(VeniceViewWriterFactory.class);
    VeniceViewWriterFactory encryptedViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Store store = mock(Store.class);
    Store encryptedStore = mock(Store.class);
    when(encryptedStore.isEncryptionEnabled()).thenReturn(true);

    StoreIngestionTaskFactory.Builder builder = StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(writerFactory)
        .setEncryptedVeniceWriterFactory(encryptedWriterFactory)
        .setVeniceViewWriterFactory(viewWriterFactory)
        .setEncryptedVeniceViewWriterFactory(encryptedViewWriterFactory);

    assertSame(builder.getVeniceWriterFactory(store), writerFactory);
    assertSame(builder.getVeniceViewWriterFactory(store), viewWriterFactory);
    assertSame(builder.getVeniceWriterFactory(encryptedStore), encryptedWriterFactory);
    assertSame(builder.getVeniceViewWriterFactory(encryptedStore), encryptedViewWriterFactory);
  }

  @Test
  public void testBaseFactoriesAreUsedWhenEncryptedFactoriesAreNotOverridden() {
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    VeniceViewWriterFactory viewWriterFactory = mock(VeniceViewWriterFactory.class);
    Store encryptedStore = mock(Store.class);
    when(encryptedStore.isEncryptionEnabled()).thenReturn(true);

    StoreIngestionTaskFactory.Builder builder = StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(writerFactory)
        .setVeniceViewWriterFactory(viewWriterFactory);

    assertSame(builder.getVeniceWriterFactory(encryptedStore), writerFactory);
    assertSame(builder.getVeniceViewWriterFactory(encryptedStore), viewWriterFactory);
  }
}
