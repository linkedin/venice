package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.MergedValueRmdRocksDBStoragePartition.HEADER_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import org.rocksdb.RocksDB;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for the merged value-RMD record format used by {@link MergedValueRmdRocksDBStoragePartition}.
 * The merged layout is {@code [schemaId 4B][valueLen 4B][valueBytes][rmdBytes]}. These tests exercise the
 * pure split/merge helpers, including the tombstone and corrupt-record edge cases.
 */
public class MergedValueRmdRocksDBStoragePartitionTest {
  private static final String DATA_BASE_DIR = Utils.getUniqueTempPath();
  private static final RocksDBThrottler ROCKSDB_THROTTLER = new RocksDBThrottler(3);

  @BeforeClass
  public void loadRocksDBLibrary() {
    // The partition class hierarchy initializes RocksDB option handles in its static initializer, which
    // requires the native library to be loaded before any of the static record-format helpers are touched.
    RocksDB.loadLibrary();
  }

  /** Builds a standard value payload: {@code [schemaId 4B][valueBytes]}. */
  private static byte[] valueWithSchemaId(int schemaId, byte[] valueBytes) {
    byte[] value = new byte[Integer.BYTES + valueBytes.length];
    ByteUtils.writeInt(value, schemaId, 0);
    System.arraycopy(valueBytes, 0, value, Integer.BYTES, valueBytes.length);
    return value;
  }

  @Test
  public void testMergeAndExtractRoundTrip() {
    byte[] value = valueWithSchemaId(5, "hello".getBytes());
    byte[] rmd = "metadata".getBytes();

    byte[] merged = MergedValueRmdRocksDBStoragePartition.mergeValueAndRmd(value, rmd);

    assertEquals(MergedValueRmdRocksDBStoragePartition.extractValue(merged), value);
    assertEquals(MergedValueRmdRocksDBStoragePartition.extractRmd(merged), rmd);

    byte[][] split = MergedValueRmdRocksDBStoragePartition.splitMergedRecord(merged);
    assertEquals(split[0], value);
    assertEquals(split[1], rmd);
  }

  @Test
  public void testValueOnlyHasNoRmd() {
    byte[] value = valueWithSchemaId(7, "world".getBytes());

    byte[] merged = MergedValueRmdRocksDBStoragePartition.mergeValueAndRmd(value, new byte[0]);

    assertEquals(MergedValueRmdRocksDBStoragePartition.extractValue(merged), value);
    assertNull(MergedValueRmdRocksDBStoragePartition.extractRmd(merged), "A value-only record has no RMD section");
  }

  @Test
  public void testTombstoneWithRmdReturnsNullValue() {
    // Mirror what deleteWithReplicationMetadata writes: schemaId=0, valueLen=0, followed by RMD bytes.
    byte[] rmd = "tombstone-rmd".getBytes();
    byte[] tombstone = new byte[HEADER_SIZE + rmd.length];
    ByteUtils.writeInt(tombstone, 0, 0);
    ByteUtils.writeInt(tombstone, 0, Integer.BYTES);
    System.arraycopy(rmd, 0, tombstone, HEADER_SIZE, rmd.length);

    assertNull(
        MergedValueRmdRocksDBStoragePartition.extractValue(tombstone),
        "A delete tombstone must read back as null even when RMD bytes are present");
    assertEquals(MergedValueRmdRocksDBStoragePartition.extractRmd(tombstone), rmd);
  }

  @Test
  public void testMergeNullValueProducesTombstoneWithRmd() {
    byte[] rmd = "rmd-only".getBytes();

    byte[] merged = MergedValueRmdRocksDBStoragePartition.mergeValueAndRmd(null, rmd);

    assertNull(MergedValueRmdRocksDBStoragePartition.extractValue(merged));
    assertEquals(MergedValueRmdRocksDBStoragePartition.extractRmd(merged), rmd);
  }

  @Test
  public void testEmptyPayloadValueIsNotTreatedAsTombstone() {
    // A legitimate value with an empty serialized payload is just [schemaId] (valueLen==0, schemaId>0). Only the
    // sentinel schemaId==0 && valueLen==0 is a tombstone, so this must round-trip rather than read back as null.
    byte[] emptyPayloadValue = valueWithSchemaId(7, new byte[0]);
    byte[] rmd = "rmd-bytes".getBytes();

    byte[] merged = MergedValueRmdRocksDBStoragePartition.mergeValueAndRmd(emptyPayloadValue, rmd);

    assertEquals(MergedValueRmdRocksDBStoragePartition.extractValue(merged), emptyPayloadValue);
    assertEquals(MergedValueRmdRocksDBStoragePartition.extractRmd(merged), rmd);
  }

  @Test
  public void testExtractFromNullOrTruncatedRecord() {
    assertNull(MergedValueRmdRocksDBStoragePartition.extractValue(null));
    assertNull(MergedValueRmdRocksDBStoragePartition.extractRmd(null));
    // Shorter than the 8-byte header.
    assertNull(MergedValueRmdRocksDBStoragePartition.extractValue(new byte[] { 1, 2, 3 }));
    assertNull(MergedValueRmdRocksDBStoragePartition.extractRmd(new byte[] { 1, 2, 3 }));
  }

  @Test
  public void testCorruptValueLengthIsHandledWithoutThrowing() {
    // Header declares a value length far larger than the actual record; must not throw.
    byte[] corrupt = new byte[HEADER_SIZE];
    ByteUtils.writeInt(corrupt, 1, 0); // schemaId
    ByteUtils.writeInt(corrupt, 1_000_000, Integer.BYTES); // bogus valueLen

    assertNull(MergedValueRmdRocksDBStoragePartition.extractValue(corrupt));
    assertNull(MergedValueRmdRocksDBStoragePartition.extractRmd(corrupt));

    // Negative declared value length.
    byte[] negative = new byte[HEADER_SIZE + 4];
    ByteUtils.writeInt(negative, 1, 0);
    ByteUtils.writeInt(negative, -10, Integer.BYTES);

    assertNull(MergedValueRmdRocksDBStoragePartition.extractValue(negative));
    assertNull(MergedValueRmdRocksDBStoragePartition.extractRmd(negative));
  }

  /**
   * The reuse-buffer {@code get(byte[], ByteBuffer)} overload (used by the Avro chunking read path) must return the
   * stripped value, identical to {@code get(byte[])}, rather than the raw merged record.
   */
  @Test
  public void testReuseBufferGetStripsMergedHeader() {
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("merged_cf"), 1);
    File storeDir = new File(DATA_BASE_DIR, storeName);
    storeDir.mkdirs();
    storeDir.deleteOnExit();
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, 0);
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(serverProps);
    VeniceServerConfig serverConfig = new VeniceServerConfig(serverProps);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    MergedValueRmdRocksDBStoragePartition partition = new MergedValueRmdRocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig);
    try {
      byte[] key = "key1".getBytes();
      byte[] value = valueWithSchemaId(5, "hello-merged-value".getBytes());
      partition.putWithReplicationMetadata(key, value, "rmd-bytes".getBytes());

      // Sanity: get(byte[]) strips the merged header to the standard value format.
      assertEquals(partition.get(key), value);

      // get(byte[], ByteBuffer) must return the SAME stripped value, not the raw merged record.
      ByteBuffer populated = partition.get(key, ByteBuffer.allocate(value.length));
      assertNotNull(populated);
      assertEquals(toBytes(populated), value);

      // A too-small reusable buffer triggers reallocation and still returns the stripped value.
      assertEquals(toBytes(partition.get(key, ByteBuffer.allocate(1))), value);

      // A missing key returns null.
      assertNull(partition.get("missing-key".getBytes(), ByteBuffer.allocate(16)));
    } finally {
      partition.drop();
      factory.close();
    }
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
