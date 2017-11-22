package com.linkedin.venice.storage.protocol;

import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ChunkedKeySuffixTest {

  @Test
  public void testChunkedKeySuffixOrdering() {
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    List<byte[]> keys = new ArrayList<>();
    List<byte[]> keysWithSuffix = new ArrayList<>();

    GUID guid = GuidUtils.getGUID(new VeniceProperties(new Properties()));
    for (int key = 0; key < 10; key++) {
      byte[] keyArray = new byte[1];
      keyArray[0] = new Integer(key).byteValue();
      keys.add(keyArray);
      for (int chunk = 0; chunk < 10; chunk++) {
        ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
        chunkedKeySuffix.isChunk = true;
        chunkedKeySuffix.chunkId = new ChunkId();
        chunkedKeySuffix.chunkId.producerGUID = guid;
        chunkedKeySuffix.chunkId.segmentNumber = 0;
        chunkedKeySuffix.chunkId.messageSequenceNumber = 0;
        chunkedKeySuffix.chunkId.chunkIndex = chunk;
        byte[] keyWithSuffix = keyWithChunkingSuffixSerializer.serialize(keyArray, chunkedKeySuffix);
        keysWithSuffix.add(keyWithSuffix);
      }
      ChunkedKeySuffix chunkedKeySuffixForManifest = new ChunkedKeySuffix();
      chunkedKeySuffixForManifest.isChunk = false;
      chunkedKeySuffixForManifest.chunkId = null;
      byte[] keyWithSuffixForManifest = keyWithChunkingSuffixSerializer.serialize(keyArray, chunkedKeySuffixForManifest);
      keysWithSuffix.add(keyWithSuffixForManifest);
    }

    Comparator<byte[]> referenceImplementationComparator = (o1, o2) -> ByteUtils.compare(o1, o2);
    List<byte[]> sortedKeys = keys.stream().sorted(referenceImplementationComparator).collect(Collectors.toList());
    List<byte[]> sortedKeysWithSuffix = keysWithSuffix.stream().sorted(referenceImplementationComparator).collect(Collectors.toList());

    Assert.assertEquals(keys, sortedKeys, "The keys should be ordered to begin with!");
    Assert.assertEquals(keysWithSuffix, sortedKeysWithSuffix, "The keys with suffix should be ordered!");

    /**
     * The following tests are to make sure that {@link BytesWritable} keeps implementing comparison correctly
     */

    Comparator<byte[]> hadoopComparator = (o1, o2) -> BytesWritable.Comparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
    List<byte[]> sortedKeysAccordingToHadoopBytesWritable = keys.stream().sorted(hadoopComparator).collect(Collectors.toList());
    List<byte[]> sortedKeysWithSuffixAccordingToHadoopBytesWritable = keysWithSuffix.stream().sorted(hadoopComparator).collect(Collectors.toList());

    Assert.assertEquals(keys, sortedKeysAccordingToHadoopBytesWritable, "The keys should be ordered according to Hadoop's BytesWritabe!");
    Assert.assertEquals(keysWithSuffix, sortedKeysWithSuffixAccordingToHadoopBytesWritable, "The keys with suffix should be ordered according to Hadoop's BytesWritabe!");

  }

}
