package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.stats.RocksDBBackgroundStats;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionJobStats;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.TableProperties;


/** Factory-owned listener; its native handle outlives every database using it. */
class RocksDBBackgroundEventListener extends AbstractEventListener {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBBackgroundEventListener.class);
  private final RocksDBBackgroundStats stats;
  private final LongSupplier nanoTime;
  private final Map<FlushKey, FlushStart> flushStarts = new ConcurrentHashMap<>();

  RocksDBBackgroundEventListener(RocksDBBackgroundStats stats) {
    this(stats, System::nanoTime);
  }

  RocksDBBackgroundEventListener(RocksDBBackgroundStats stats, LongSupplier nanoTime) {
    super(
        EnabledEventCallback.ON_FLUSH_BEGIN,
        EnabledEventCallback.ON_FLUSH_COMPLETED,
        EnabledEventCallback.ON_COMPACTION_COMPLETED);
    this.stats = stats;
    this.nanoTime = nanoTime;
  }

  @Override
  public void onFlushBegin(RocksDB db, FlushJobInfo info) {
    // Failed flushes and mempurges may never complete. Replacement bounds retained state per native thread/CF.
    flushStarts.put(new FlushKey(db, info), new FlushStart(info.getJobId(), nanoTime.getAsLong()));
  }

  @Override
  public void onFlushCompleted(RocksDB db, FlushJobInfo info) {
    stats.recordFlushCompleted();
    FlushKey key = new FlushKey(db, info);
    FlushStart start = flushStarts.get(key);
    if (start != null && start.jobId == info.getJobId() && flushStarts.remove(key, start)) {
      stats.recordFlushDuration(nanoTime.getAsLong() - start.nanoTime);
    } else {
      // A completion can be committed by another flush after its original begin was superseded.
      LOGGER.warn("No matching flush begin for completed job {}; omitting duration sample", info.getJobId());
    }
    TableProperties properties = info.getTableProperties();
    if (properties != null) {
      stats.recordFlushBytes(properties.getRawKeySize() + properties.getRawValueSize());
    } else {
      LOGGER.warn("No table properties for completed flush job {}; omitting byte sample", info.getJobId());
    }
  }

  @Override
  public void onCompactionCompleted(RocksDB db, CompactionJobInfo info) {
    stats.recordCompactionCompleted();
    // JNI returns an owned copy. The callback's CompactionJobInfo itself is owned by JNI.
    try (CompactionJobStats jobStats = info.stats()) {
      if (jobStats != null) {
        stats.recordCompactionStats(jobStats.elapsedMicros(), jobStats.totalInputBytes(), jobStats.totalOutputBytes());
      } else {
        LOGGER.warn("No native statistics for completed compaction job {}; omitting duration and bytes", info.jobId());
      }
    }
  }

  void databaseClosed(long databaseHandle) {
    flushStarts.keySet().removeIf(key -> key.databaseHandle == databaseHandle);
  }

  @Override
  public void close() {
    super.close();
    flushStarts.clear();
  }

  private static class FlushStart {
    private final int jobId;
    private final long nanoTime;

    FlushStart(int jobId, long nanoTime) {
      this.jobId = jobId;
      this.nanoTime = nanoTime;
    }
  }

  private static class FlushKey {
    private final long databaseHandle;
    private final long columnFamilyId;
    private final long threadId;

    FlushKey(RocksDB db, FlushJobInfo info) {
      databaseHandle = db.getNativeHandle();
      columnFamilyId = info.getColumnFamilyId();
      threadId = info.getThreadId();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof FlushKey)) {
        return false;
      }
      FlushKey key = (FlushKey) other;
      return databaseHandle == key.databaseHandle && columnFamilyId == key.columnFamilyId && threadId == key.threadId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(databaseHandle, columnFamilyId, threadId);
    }
  }
}
