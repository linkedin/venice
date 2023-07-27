package com.linkedin.venice.writer;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;


/**
 *  The {@link VeniceWriter}, upon detecting an instance of this class being passed to it, will always call
 *  {@link #setChunkingInfo(byte[], ByteBuffer[], ChunkedValueManifest, ByteBuffer[], ChunkedValueManifest, ChunkedValueManifest, ChunkedValueManifest)} whenever
 *  processing a {@link MessageType#PUT}, whether it is chunked or not.
 */
public interface ChunkAwareCallback extends PubSubProducerCallback {
  /**
   * For all PUT operations, the {@param key} is guaranteed to be passed via this function, whether chunking
   * is enabled or not, and whether the value is chunked or not. The other two parameters are null if the value
   * is not chunked.
   *
   * @param key A byte[] corresponding to the top-level key written to Kafka, potentially including a chunking suffix
   * @param valueChunks An array of {@link ByteBuffer} where the backing array has sufficient headroom to prepend Venice's header
   * @param chunkedValueManifest The {@link ChunkedValueManifest} of the new chunked value
   * @param rmdChunks An array of {@link ByteBuffer} where the backing array has sufficient headroom to prepend Venice's header
   * @param chunkedRmdManifest The {@link ChunkedValueManifest} of the new chunked RMD
   * @param oldValueManifest The {@link ChunkedValueManifest} of the previous chunked value
   * @param oldRmdManifest The {@link ChunkedValueManifest} of the previous chunked RMD
   */
  void setChunkingInfo(
      byte[] key,
      ByteBuffer[] valueChunks,
      ChunkedValueManifest chunkedValueManifest,
      ByteBuffer[] rmdChunks,
      ChunkedValueManifest chunkedRmdManifest,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest);
}
