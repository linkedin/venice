package com.linkedin.davinci.store.rocksdb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.RocksDBBackgroundStats;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionJobStats;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.TableProperties;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RocksDBBackgroundEventListenerTest {
  @BeforeClass
  public void loadLibrary() {
    RocksDB.loadLibrary();
  }

  @Test
  public void testFlushUsesMonotonicCallbackIntervalAndRawBytes() {
    RocksDBBackgroundStats stats = mock(RocksDBBackgroundStats.class);
    AtomicLong clock = new AtomicLong(100);
    RocksDB db = database(1);
    FlushJobInfo info = flush(7, 2, 3);
    TableProperties properties = mock(TableProperties.class);
    when(properties.getRawKeySize()).thenReturn(40L);
    when(properties.getRawValueSize()).thenReturn(60L);
    when(info.getTableProperties()).thenReturn(properties);
    try (RocksDBBackgroundEventListener listener = new RocksDBBackgroundEventListener(stats, clock::get)) {
      listener.onFlushBegin(db, info);
      clock.set(2_500_100);
      listener.onFlushCompleted(db, info);
      verify(stats).recordFlushDuration(2_500_000);
      verify(stats).recordFlushCompleted();
      verify(stats).recordFlushBytes(100);
    }
  }

  @Test
  public void testMissingSupersededAndClosedDatabaseBeginsDoNotInventDurations() {
    RocksDBBackgroundStats stats = mock(RocksDBBackgroundStats.class);
    AtomicLong clock = new AtomicLong(100);
    RocksDB db = database(1);
    RocksDB anotherDb = database(2);
    FlushJobInfo first = flush(1, 0, 3);
    FlushJobInfo second = flush(2, 0, 3);
    try (RocksDBBackgroundEventListener listener = new RocksDBBackgroundEventListener(stats, clock::get)) {
      listener.onFlushBegin(db, first);
      listener.onFlushBegin(db, second);
      listener.onFlushCompleted(db, first);
      listener.onFlushCompleted(anotherDb, second);
      listener.databaseClosed(1);
      listener.onFlushCompleted(db, second);
      verify(stats, times(3)).recordFlushCompleted();
      verify(stats, never()).recordFlushDuration(org.mockito.ArgumentMatchers.anyLong());
      verify(stats, never()).recordFlushBytes(org.mockito.ArgumentMatchers.anyLong());
    }
  }

  @Test
  public void testFlushIdentitySeparatesDatabaseColumnFamilyAndThread() {
    RocksDBBackgroundStats stats = mock(RocksDBBackgroundStats.class);
    AtomicLong clock = new AtomicLong(100);
    RocksDB db = database(1);
    RocksDB anotherDb = database(2);
    FlushJobInfo first = flush(1, 0, 3);
    FlushJobInfo otherColumnFamily = flush(1, 1, 3);
    FlushJobInfo otherThread = flush(1, 0, 4);
    try (RocksDBBackgroundEventListener listener = new RocksDBBackgroundEventListener(stats, clock::get)) {
      listener.onFlushBegin(db, first);
      listener.onFlushBegin(db, otherColumnFamily);
      listener.onFlushBegin(db, otherThread);
      listener.onFlushBegin(anotherDb, first);
      clock.set(200);
      listener.onFlushCompleted(db, first);
      listener.onFlushCompleted(db, otherColumnFamily);
      listener.onFlushCompleted(db, otherThread);
      listener.onFlushCompleted(anotherDb, first);
      verify(stats, times(4)).recordFlushDuration(100);
    }
  }

  @Test
  public void testCompactionUsesNativeElapsedWallTimeAndClosesOwnedStatsCopy() {
    RocksDBBackgroundStats stats = mock(RocksDBBackgroundStats.class);
    CompactionJobInfo info = mock(CompactionJobInfo.class);
    CompactionJobStats jobStats = mock(CompactionJobStats.class);
    when(info.stats()).thenReturn(jobStats);
    when(jobStats.elapsedMicros()).thenReturn(1250L);
    when(jobStats.totalInputBytes()).thenReturn(200L);
    when(jobStats.totalOutputBytes()).thenReturn(150L);
    try (RocksDBBackgroundEventListener listener = new RocksDBBackgroundEventListener(stats)) {
      listener.onCompactionCompleted(database(1), info);
      verify(stats).recordCompactionCompleted();
      verify(stats).recordCompactionStats(1250, 200, 150);
      verify(jobStats).close();
      verify(info, never()).close();
    }
  }

  @Test
  public void testCompactionWithoutStatsOnlyCountsCompletion() {
    RocksDBBackgroundStats stats = mock(RocksDBBackgroundStats.class);
    try (RocksDBBackgroundEventListener listener = new RocksDBBackgroundEventListener(stats)) {
      listener.onCompactionCompleted(database(1), mock(CompactionJobInfo.class));
      verify(stats).recordCompactionCompleted();
      verify(stats, never()).recordCompactionStats(
          org.mockito.ArgumentMatchers.anyLong(),
          org.mockito.ArgumentMatchers.anyLong(),
          org.mockito.ArgumentMatchers.anyLong());
    }
  }

  private RocksDB database(long handle) {
    RocksDB db = mock(RocksDB.class);
    when(db.getNativeHandle()).thenReturn(handle);
    return db;
  }

  private FlushJobInfo flush(int jobId, long columnFamilyId, long threadId) {
    FlushJobInfo info = mock(FlushJobInfo.class);
    when(info.getJobId()).thenReturn(jobId);
    when(info.getColumnFamilyId()).thenReturn(columnFamilyId);
    when(info.getThreadId()).thenReturn(threadId);
    return info;
  }
}
