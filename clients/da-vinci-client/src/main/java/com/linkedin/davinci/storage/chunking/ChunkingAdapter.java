package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import org.apache.avro.io.BinaryDecoder;


/**
 * This interface allows each code path which needs to interact with chunked values
 * to optimize the way the re-assembly is handled so that the final form in which
 * the {@type VALUE} is consumed is generated as efficiently as possible, via the
 * use of a temporary {@link CHUNKS_CONTAINER}.
 */
public interface ChunkingAdapter<CHUNKS_CONTAINER, VALUE> {
  /**
   * Used to wrap a small {@param fullBytes} value fetched from the storage engine into the right type of {@param VALUE}
   * class needed by the query code.
   * <p>
   * The following parameters are mandatory:
   *
   * @param fullBytes      includes both the schema ID header and the payload
   *                       <p>
   *                       The following parameters can be ignored, by implementing
   *                       {@link #constructValue(int, byte[])}:
   * @param bytesLength
   * @param reusedValue    a previous instance of {@type VALUE} to be re-used in order to minimize GC
   * @param reusedDecoder  a previous instance of {@link BinaryDecoder} to be re-used in order to minimize GC
   * @param responseStats  the {@link ReadResponseStats} which carries certain metrics to be recorded at the end
   * @param writerSchemaId schema used to serialize the value
   * @param readerSchemaId
   */
  default VALUE constructValue(
      byte[] fullBytes,
      int bytesLength,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats responseStats,
      int writerSchemaId,
      int readerSchemaId,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor) {
    return constructValue(writerSchemaId, fullBytes);
  }

  default VALUE constructValue(
      byte[] valueOnlyBytes,
      int offset,
      int bytesLength,
      RecordDeserializer<VALUE> recordDeserializer,
      VeniceCompressor veniceCompressor) {
    throw new VeniceException("Not implemented.");
  }

  /**
   * This function can be implemented by the adapters which need fewer parameters.
   */
  default VALUE constructValue(int schemaId, byte[] fullBytes) {
    throw new VeniceException("Not implemented.");
  }

  /**
   * Used to incrementally add a {@param valueChunk} into the {@param CHUNKS_CONTAINER}
   * container.
   */
  void addChunkIntoContainer(CHUNKS_CONTAINER container, int chunkIndex, byte[] valueChunk);

  /**
   * Used to construct the right kind of {@param CHUNKS_CONTAINER} container (according to
   * the query code) to hold a large value which needs to be incrementally re-assembled from many
   * smaller chunks.
   */
  CHUNKS_CONTAINER constructChunksContainer(ChunkedValueManifest chunkedValueManifest);

  /**
   * Used to wrap a large value re-assembled with the use of a {@param CHUNKS_CONTAINER} into the right type of
   * {@param VALUE} class needed by the query code.
   * <p>
   * The following parameters are mandatory:
   *
   * @param chunksContainer temporary {@type CHUNKS_CONTAINER}, obtained from
   *                        {@link #constructChunksContainer(ChunkedValueManifest)}
   *                        <p>
   *                        The following parameters can be ignored, by implementing
   *                        {@link #constructValue(int, Object)}:
   * @param reusedValue     a previous instance of {@type VALUE} to be re-used in order to minimize GC
   * @param reusedDecoder   a previous instance of {@link BinaryDecoder} to be re-used in order to minimize GC
   * @param responseStats  the {@link ReadResponseStats} which carries certain metrics to be recorded at the end
   * @param writerSchemaId  of the user's value
   * @param readerSchemaId
   */
  default VALUE constructValue(
      CHUNKS_CONTAINER chunksContainer,
      VALUE reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats responseStats,
      int writerSchemaId,
      int readerSchemaId,
      StoreDeserializerCache<VALUE> storeDeserializerCache,
      VeniceCompressor compressor) {
    return constructValue(writerSchemaId, chunksContainer);
  }

  /**
   * This function can be implemented by the adapters which need fewer parameters.
   */
  default VALUE constructValue(int schemaId, CHUNKS_CONTAINER chunksContainer) {
    throw new VeniceException("Not implemented.");
  }
}
