package com.linkedin.venice.storage.protocol;

import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ChunkedKeySuffixTest {
  /**
   * This test verifies that lexicographical ordering is maintained properly in our keys, which is
   * important in order to minimize the BDB overhead incurred from out-of-order ingestion.
   *
   * More concretely, this test verifies that the Hadoop MapReduce shuffling process should order
   * records properly, via {@link BytesWritable.Comparator#compareBytes(byte[], int, int, byte[], int, int)},
   * as compared to our own reference implementation: {@link ByteUtils#compare(byte[], byte[])}.
   *
   * This guarantee should be maintained even when message chunking is enabled, which is why the
   * {@link ChunkedKeySuffix} is appended to the end of the regular key.
   *
   * N.B.: There is an edge case where a key combined to a {@link ChunkedKeySuffix} will not yield
   *       a fully ordered stream of records. This would happen if the user-provided key has variable
   *       length, and the end part of the key content happens to collide with the beginning of the
   *       chunking metadata. The format overall, when taking entire keys into account, is guaranteed
   *       to not collide, which is critical in order to avoid data corruption, but ordering is not
   *       fully guaranteed. Since this not a correctness problem, but rather an efficiency concern,
   *       it is considered an acceptable tradeoff. If in the future we encounter stores where the
   *       data patterns cause these kinds of out of order issue at a high rate, to the point where
   *       it causes efficiency problems in BDB, then we can consider improving the scheme, but
   *       unless we encounter such problem, it's not worth introducing more complexity into the
   *       design for now.
   */
  @Test
  public void testChunkedKeySuffixOrdering() {
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    List<byte[]> keys = new ArrayList<>();
    List<byte[]> keysWithSuffix = new ArrayList<>();

    GUID guid = GuidUtils.getGUID(VeniceProperties.empty());
    for (int key = 0; key < 10; key++) {
      byte[] keyArray = new byte[1];
      keyArray[0] = (byte) key;
      keys.add(keyArray);
      for (int chunk = 0; chunk < 10; chunk++) {
        ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
        chunkedKeySuffix.isChunk = true;
        chunkedKeySuffix.chunkId = new ChunkId();
        chunkedKeySuffix.chunkId.producerGUID = guid;
        chunkedKeySuffix.chunkId.segmentNumber = 0;
        chunkedKeySuffix.chunkId.messageSequenceNumber = 0;
        chunkedKeySuffix.chunkId.chunkIndex = chunk;
        byte[] keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(keyArray, chunkedKeySuffix).array();
        keysWithSuffix.add(keyWithSuffix);
      }
      ChunkedKeySuffix chunkedKeySuffixForManifest = new ChunkedKeySuffix();
      chunkedKeySuffixForManifest.isChunk = false;
      chunkedKeySuffixForManifest.chunkId = null;
      byte[] keyWithSuffixForManifest =
          keyWithChunkingSuffixSerializer.serializeChunkedKey(keyArray, chunkedKeySuffixForManifest).array();
      keysWithSuffix.add(keyWithSuffixForManifest);
    }

    Comparator<byte[]> referenceImplementationComparator = (o1, o2) -> ByteUtils.compare(o1, o2);
    List<byte[]> sortedKeys = keys.stream().sorted(referenceImplementationComparator).collect(Collectors.toList());
    List<byte[]> sortedKeysWithSuffix =
        keysWithSuffix.stream().sorted(referenceImplementationComparator).collect(Collectors.toList());

    Assert.assertEquals(keys, sortedKeys, "The keys should be ordered to begin with!");
    Assert.assertEquals(keysWithSuffix, sortedKeysWithSuffix, "The keys with suffix should be ordered!");

    /**
     * The following tests are to make sure that {@link BytesWritable} keeps implementing comparison correctly
     */

    Comparator<byte[]> hadoopComparator =
        (o1, o2) -> BytesWritable.Comparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
    List<byte[]> sortedKeysAccordingToHadoopBytesWritable =
        keys.stream().sorted(hadoopComparator).collect(Collectors.toList());
    List<byte[]> sortedKeysWithSuffixAccordingToHadoopBytesWritable =
        keysWithSuffix.stream().sorted(hadoopComparator).collect(Collectors.toList());

    Assert.assertEquals(
        keys,
        sortedKeysAccordingToHadoopBytesWritable,
        "The keys should be ordered according to Hadoop's BytesWritabe!");
    Assert.assertEquals(
        keysWithSuffix,
        sortedKeysWithSuffixAccordingToHadoopBytesWritable,
        "The keys with suffix should be ordered according to Hadoop's BytesWritabe!");
  }
}
