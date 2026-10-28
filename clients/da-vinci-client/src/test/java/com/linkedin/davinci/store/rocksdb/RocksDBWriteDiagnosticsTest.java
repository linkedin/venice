package com.linkedin.davinci.store.rocksdb;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.utils.IngestionOperationContext;
import com.linkedin.davinci.utils.IngestionOperationContext.Phase;
import com.linkedin.davinci.utils.IngestionOperationContext.Snapshot;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.mockito.MockedConstruction;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RocksDBWriteDiagnosticsTest {
  @DataProvider
  public Object[][] writeOperations() {
    List<Object[]> cases = new ArrayList<>();
    for (String operation: new String[] { "put", "putBuffer", "putWithRmd", "putRmd", "delete", "deleteWithRmd", "sync",
        "syncEngineLock" }) {
      for (boolean fail: new boolean[] { false, true }) {
        for (boolean tracked: new boolean[] { false, true }) {
          cases.add(new Object[] { operation, fail, tracked });
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "writeOperations")
  public void testSynchronizationAndNativeBoundaries(String operation, boolean fail, boolean tracked) throws Exception {
    String storeVersion = "diagnostic_store_v1";
    int partitionId = 1;
    File directory = Utils.getTempDataDirectory();
    VeniceProperties properties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(new VeniceServerConfig(properties));
    RocksDB db = mock(RocksDB.class);
    RocksDBThrottler throttler = mock(RocksDBThrottler.class);
    doAnswer(invocation -> {
      List<ColumnFamilyHandle> handles = invocation.getArgument(3);
      handles.add(mock(ColumnFamilyHandle.class));
      handles.add(mock(ColumnFamilyHandle.class));
      return db;
    }).when(throttler).open(any(), any(), any(), any());
    ReplicationMetadataRocksDBStoragePartition partition = spy(
        new ReplicationMetadataRocksDBStoragePartition(
            new StoragePartitionConfig(storeVersion, partitionId),
            factory,
            directory.getAbsolutePath(),
            null,
            throttler,
            new RocksDBServerConfig(properties)));
    AbstractStorageEngine<?> engine = mock(
        AbstractStorageEngine.class,
        withSettings().useConstructor(storeVersion, null, null).defaultAnswer(CALLS_REAL_METHODS));
    doReturn(partition).when(engine).createStoragePartition(any());
    engine.addStoragePartitionIfAbsent(partitionId);
    IngestionOperationContext context = new IngestionOperationContext();
    Snapshot initialSnapshot = context.snapshot();
    AtomicReference<Thread> workerThread = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch enteredNative = new CountDownLatch(1);
    CountDownLatch releaseNative = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicInteger nativeCalls = new AtomicInteger();
    Answer<Void> nativeCall = invocation -> {
      Assert.assertTrue(Thread.holdsLock(partition), "Native writes must retain the partition's lifecycle guard");
      Assert.assertEquals(IngestionOperationContext.isTracking(), tracked);
      Assert.assertEquals(nativeCalls.getAndIncrement(), 0, "The original native operation must execute only once");
      enteredNative.countDown();
      Assert.assertTrue(releaseNative.await(10, SECONDS), "Simulated native call was not released");
      if (fail) {
        throw new RocksDBException("simulated write failure");
      }
      return null;
    };
    doAnswer(nativeCall).when(db)
        .put(any(WriteOptions.class), any(byte[].class), anyInt(), anyInt(), any(byte[].class), anyInt(), anyInt());
    doAnswer(nativeCall).when(db)
        .put(any(ColumnFamilyHandle.class), any(WriteOptions.class), any(byte[].class), any(byte[].class));
    doAnswer(nativeCall).when(db).write(any(WriteOptions.class), any(WriteBatch.class));
    doAnswer(nativeCall).when(db).delete(any(byte[].class));
    doAnswer(nativeCall).when(db).flush(any(), anyList());
    Phase storagePhase;
    Phase nativePhase;
    switch (operation) {
      case "putWithRmd":
        storagePhase = Phase.STORAGE_PUT_WITH_RMD;
        nativePhase = Phase.ROCKSDB_WRITE;
        break;
      case "putRmd":
        storagePhase = Phase.STORAGE_PUT_RMD;
        nativePhase = Phase.ROCKSDB_PUT;
        break;
      case "delete":
        storagePhase = Phase.STORAGE_DELETE;
        nativePhase = Phase.ROCKSDB_DELETE;
        break;
      case "deleteWithRmd":
        storagePhase = Phase.STORAGE_DELETE_WITH_RMD;
        nativePhase = Phase.ROCKSDB_WRITE;
        break;
      case "sync":
      case "syncEngineLock":
        storagePhase = Phase.STORAGE_SYNC;
        nativePhase = Phase.ROCKSDB_FLUSH;
        break;
      default:
        storagePhase = Phase.STORAGE_PUT;
        nativePhase = Phase.ROCKSDB_PUT;
    }
    try {
      Future<?> work;
      synchronized ("syncEngineLock".equals(operation) ? engine : partition) {
        work = executor.submit(() -> {
          workerThread.set(Thread.currentThread());
          Assert.assertFalse(IngestionOperationContext.isTracking());
          if (tracked) {
            context.start(Phase.PROCESS_RECORD, storeVersion, partitionId);
          }
          boolean succeeded = false;
          // Batch construction is mocked on this thread too: no native write or hang is involved.
          try (MockedConstruction<WriteBatch> ignored = mockConstruction(WriteBatch.class)) {
            started.countDown();
            try {
              invokeWrite(engine, partitionId, operation);
              Assert.assertFalse(fail, "Storage failure must propagate");
              succeeded = true;
            } catch (VeniceException e) {
              Assert.assertTrue(fail);
              Assert.assertTrue(e.getCause() instanceof RocksDBException);
            }
            Snapshot completed = context.snapshot();
            if (tracked) {
              Assert.assertEquals(completed.operation.phase, Phase.PROCESS_RECORD);
              Assert.assertEquals(completed.lastCompleted.phase, storagePhase);
              Assert.assertEquals(completed.lastSucceeded, !fail);
            } else {
              Assert.assertSame(completed, initialSnapshot);
            }
          } finally {
            if (tracked) {
              context.finish(succeeded);
            }
          }
        });
        Assert.assertTrue(started.await(10, SECONDS));
        TestUtils.waitForNonDeterministicAssertion(5, SECONDS, () -> {
          Assert.assertEquals(workerThread.get().getState(), Thread.State.BLOCKED);
          if (tracked) {
            Assert.assertEquals(context.snapshot().operation.phase, storagePhase);
          } else {
            Assert.assertSame(context.snapshot(), initialSnapshot);
          }
        });
        Assert.assertEquals(enteredNative.getCount(), 1);
      }
      Assert.assertTrue(enteredNative.await(10, SECONDS));
      Snapshot blocked = context.snapshot();
      if (tracked) {
        Assert.assertEquals(blocked.operation.phase, nativePhase);
        Assert.assertEquals(blocked.operation.storeVersion, storeVersion);
        Assert.assertEquals(blocked.operation.partition, partitionId);
        Assert.assertTrue(blocked.operation.startedAtMs > 0);
        Assert.assertTrue(blocked.operation.ageMs() >= 0);
      } else {
        Assert.assertSame(blocked, initialSnapshot);
      }
      IngestionOperationContext.track(Phase.STORAGE_DELETE, "other_store_v1", 2, () -> null);
      Assert.assertSame(context.snapshot(), blocked, "Another thread must not overwrite this drainer's breadcrumbs");
      releaseNative.countDown();
      work.get(10, SECONDS);
      verify(partition, tracked ? times(1) : never()).trackWrite(eq(nativePhase), same(db), any());
      Assert.assertEquals(nativeCalls.get(), 1);
      Assert.assertNull(context.snapshot().root);
      Assert.assertNull(context.snapshot().operation);
      if (tracked) {
        Assert.assertEquals(context.snapshot().lastSucceeded, !fail);
      } else {
        Assert.assertSame(context.snapshot(), initialSnapshot);
      }
      executor.submit(() -> {
        Assert
            .assertFalse(IngestionOperationContext.isTracking(), "A reused worker must not retain tracking enablement");
        IngestionOperationContext.track(Phase.STORAGE_DELETE, "other_store_v1", 2, () -> null);
        Assert.assertNull(context.snapshot().operation, "A reused worker must not retain its previous context");
      }).get(10, SECONDS);
    } finally {
      releaseNative.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, SECONDS));
      partition.close();
      factory.close();
      FileUtils.deleteDirectory(directory);
    }
  }

  private void invokeWrite(AbstractStorageEngine<?> engine, int partition, String operation) {
    byte[] key = new byte[] { 1 };
    byte[] value = new byte[] { 2 };
    byte[] metadata = new byte[] { 3 };
    switch (operation) {
      case "put":
        engine.put(partition, key, value);
        break;
      case "putBuffer":
        engine.put(partition, key, ByteBuffer.wrap(value));
        break;
      case "putWithRmd":
        engine.putWithReplicationMetadata(partition, key, ByteBuffer.wrap(value), metadata);
        break;
      case "putRmd":
        engine.putReplicationMetadata(partition, key, metadata);
        break;
      case "delete":
        engine.delete(partition, key);
        break;
      case "deleteWithRmd":
        engine.deleteWithReplicationMetadata(partition, key, metadata);
        break;
      default:
        engine.sync(partition);
    }
  }
}
