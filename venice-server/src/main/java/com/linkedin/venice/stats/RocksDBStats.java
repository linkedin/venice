package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;


/**
 * Check {@link TickerType} to find more details about RocksDB metrics.
 */
public class RocksDBStats extends AbstractVeniceStats {
  private Statistics rocksDBStat;

  private final Sensor blockCacheMiss;
  private final Sensor blockCacheHit;
  private final Sensor blockCacheAdd;
  private final Sensor blockCacheAddFailures;
  private final Sensor blockCacheIndexMiss;
  private final Sensor blockCacheIndexHit;
  private final Sensor blockCacheIndexAdd;
  private final Sensor blockCacheIndexBytesInsert;
  private final Sensor blockCacheIndexBytesEvict;
  private final Sensor blockCacheFilterMiss;
  private final Sensor blockCacheFilterHit;
  private final Sensor blockCacheFilterAdd;
  private final Sensor blockCacheFilterBytesInsert;
  private final Sensor blockCacheFilterBytesEvict;
  private final Sensor blockCacheDataMiss;
  private final Sensor blockCacheDataHit;
  private final Sensor blockCacheDataAdd;
  private final Sensor blockCacheDataBytesInsert;
  private final Sensor blockCacheBytesRead;
  private final Sensor blockCacheBytesWrite;
  private final Sensor bloomFilterUseful;
  private final Sensor memtableHit;
  private final Sensor memtableMiss;
  private final Sensor getHitL0;
  private final Sensor getHitL1;
  private final Sensor getHitL2AndUp;


  public RocksDBStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    this.blockCacheMiss = registerSensor("rocksdb_block_cache_miss", TickerType.BLOCK_CACHE_MISS);
    this.blockCacheHit = registerSensor("rocksdb_block_cache_hit", TickerType.BLOCK_CACHE_HIT);
    this.blockCacheAdd = registerSensor("rocksdb_block_cache_add", TickerType.BLOCK_CACHE_ADD);
    this.blockCacheAddFailures = registerSensor("rocksdb_block_cache_add_failures", TickerType.BLOCK_CACHE_ADD_FAILURES);
    this.blockCacheIndexMiss = registerSensor("rocksdb_block_cache_index_miss", TickerType.BLOCK_CACHE_INDEX_MISS);
    this.blockCacheIndexHit = registerSensor("rocksdb_block_cache_index_hit", TickerType.BLOCK_CACHE_INDEX_HIT);
    this.blockCacheIndexAdd = registerSensor("rocksdb_block_cache_index_add", TickerType.BLOCK_CACHE_INDEX_ADD);
    this.blockCacheIndexBytesInsert = registerSensor("rocksdb_block_cache_index_bytes_insert", TickerType.BLOCK_CACHE_INDEX_BYTES_INSERT);
    this.blockCacheIndexBytesEvict = registerSensor("rocksdb_block_cache_index_bytes_evict", TickerType.BLOCK_CACHE_INDEX_BYTES_EVICT);
    this.blockCacheFilterMiss = registerSensor("rocksdb_block_cache_filter_miss", TickerType.BLOCK_CACHE_FILTER_MISS);
    this.blockCacheFilterHit = registerSensor("rocksdb_block_cache_filter_hit", TickerType.BLOCK_CACHE_FILTER_HIT);
    this.blockCacheFilterAdd = registerSensor("rocksdb_block_cache_filter_add", TickerType.BLOCK_CACHE_FILTER_ADD);
    this.blockCacheFilterBytesInsert = registerSensor("rocksdb_block_cache_filter_bytes_insert", TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT);
    this.blockCacheFilterBytesEvict = registerSensor("rocksdb_block_cache_filter_bytes_evict", TickerType.BLOCK_CACHE_FILTER_BYTES_EVICT);
    this.blockCacheDataMiss = registerSensor("rocksdb_block_cache_data_miss", TickerType.BLOCK_CACHE_DATA_MISS);
    this.blockCacheDataHit = registerSensor("rocksdb_block_cache_data_hit", TickerType.BLOCK_CACHE_DATA_HIT);
    this.blockCacheDataAdd = registerSensor("rocksdb_block_cache_data_add", TickerType.BLOCK_CACHE_DATA_ADD);
    this.blockCacheDataBytesInsert = registerSensor("rocksdb_block_cache_data_bytes_insert", TickerType.BLOCK_CACHE_DATA_BYTES_INSERT);
    this.blockCacheBytesRead = registerSensor("rocksdb_block_cache_bytes_read", TickerType.BLOCK_CACHE_BYTES_READ);
    this.blockCacheBytesWrite = registerSensor("rocksdb_block_cache_bytes_write", TickerType.BLOCK_CACHE_BYTES_WRITE);
    this.bloomFilterUseful = registerSensor("rocksdb_bloom_filter_useful", TickerType.BLOOM_FILTER_USEFUL);
    this.memtableHit = registerSensor("rocksdb_memtable_hit", TickerType.MEMTABLE_HIT);
    this.memtableMiss = registerSensor("rocksdb_memtable_miss", TickerType.MEMTABLE_MISS);
    this.getHitL0 = registerSensor("rocksdb_get_hit_l0", TickerType.GET_HIT_L0);
    this.getHitL1 = registerSensor("rocksdb_get_hit_l1", TickerType.GET_HIT_L1);
    this.getHitL2AndUp = registerSensor("rocksdb_get_hit_l2_and_up", TickerType.GET_HIT_L2_AND_UP);
  }

  private Sensor registerSensor(String sensorName, TickerType tickerType) {
    return registerSensor(sensorName, new Gauge(() -> {
      if (rocksDBStat != null) {
        return rocksDBStat.getTickerCount(tickerType);
      }
      return -1;
    }));
  }

  public void setRocksDBStat(Statistics stat) {
    if (this.rocksDBStat != null) {
      throw new VeniceException("'rocksDBStat' has already been initialized");
    }
    this.rocksDBStat = stat;
  }
}
