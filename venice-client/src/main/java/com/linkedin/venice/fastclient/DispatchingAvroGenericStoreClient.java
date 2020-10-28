package com.linkedin.venice.fastclient;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.client.store.AbstractAvroStoreClient.*;


/**
 * This class is in charge of routing and serialization/de-serialization.
 */
public class DispatchingAvroGenericStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private static final Logger LOGGER = Logger.getLogger(DispatchingAvroGenericStoreClient.class);
  private static final String URI_SEPARATOR = "/";
  private static Executor DESERIALIZATION_EXECUTOR = AbstractAvroStoreClient.getDefaultDeserializationExecutor();

  private final StoreMetadata metadata;
  private final int requiredReplicaCount;

  private final ClientConfig config;
  private final TransportClient transportClient;
  private final Executor deserializationExecutor;

  // Key serializer
  private final RecordSerializer<K> keySerializer;


  public DispatchingAvroGenericStoreClient(StoreMetadata metadata, ClientConfig config) {
    this.metadata = metadata;
    this.config = config;
    this.transportClient = new R2TransportClient(config.getR2Client());

    // Initialize key serializer
    this.keySerializer = FastSerializerDeserializerFactory.getAvroGenericSerializer(getKeySchema());

    if (config.isSpeculativeQueryEnabled()) {
      this.requiredReplicaCount = 2;
    } else {
      this.requiredReplicaCount = 1;
    }

    this.deserializationExecutor = Optional.ofNullable(config.getDeserializationExecutor())
        .orElse(DESERIALIZATION_EXECUTOR);
  }

  protected StoreMetadata getStoreMetadata() {
    return metadata;
  }


  private String composeURIForSingleGet(GetRequestContext requestContext, K key) {
    int currentVersion = metadata.getCurrentStoreVersion();
    if (currentVersion <= 0) {
      throw new VeniceClientException("No available current version, please do a push first");
    }
    String resourceName = metadata.getStoreName() + "_v" + currentVersion;
    long beforeSerializationTimeStamp = System.nanoTime();
    byte[] keyBytes = keySerializer.serialize(key, threadLocalReusableObjects.get().binaryEncoder, threadLocalReusableObjects.get().byteArrayOutputStream);
    requestContext.requestSerializationTime = LatencyUtils.getLatencyInMS(beforeSerializationTimeStamp);
    int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
    String b64EncodedKeyBytes = EncodingUtils.base64EncodeToString(keyBytes);
    StringBuilder sb = new StringBuilder();
    sb.append(URI_SEPARATOR)
        .append(AbstractAvroStoreClient.TYPE_STORAGE).append(URI_SEPARATOR)
        .append(resourceName).append(URI_SEPARATOR)
        .append(partitionId).append(URI_SEPARATOR)
        .append(b64EncodedKeyBytes)
        .append(AbstractAvroStoreClient.B64_FORMAT);

    requestContext.currentVersion = currentVersion;
    requestContext.partitionId = partitionId;

    return sb.toString();
  }


  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    String uri = composeURIForSingleGet(requestContext, key);
    int currentVersion = requestContext.currentVersion;
    int partitionId = requestContext.partitionId;

    long timestampBeforeSendingRequest = System.nanoTime();
    List<String> replicas = metadata.getReplicas(currentVersion, partitionId, requiredReplicaCount);
    if (replicas.isEmpty()) {
      requestContext.noAvailableReplica = true;
      throw new VeniceClientException("No available replica for store: " + getStoreName() + ", version: "
          + currentVersion + ", partition: " + partitionId);
    }

    /**
     * This atomic variable is used to indicate whether a faster response has returned or not.
     */
    AtomicBoolean receivedSuccessfulResponse = new AtomicBoolean(false);
    CompletableFuture<V> valueFuture = new CompletableFuture<>();

    List<CompletableFuture<TransportClientResponse>> transportFutures = new LinkedList<>();
    for (String replica : replicas) {
      try {
        metadata.sentRequestToInstance(replica, currentVersion, partitionId);
        String url = replica + uri;
        CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(url);
        transportFutures.add(transportFuture);
        transportFuture.handleAsync((response, throwable) -> {
          if (throwable != null) {
            HttpStatus statusCode = (throwable instanceof VeniceClientHttpException) ?
                HttpStatus.fromCode(((VeniceClientHttpException) throwable).getHttpStatus()) :
                HttpStatus.S_500_INTERNAL_SERVER_ERROR;
            metadata.receivedResponseFromInstance(replica, currentVersion, partitionId, statusCode);
          } else if (null == response) {
            metadata.receivedResponseFromInstance(replica, currentVersion, partitionId, HttpStatus.S_404_NOT_FOUND);
            if (!receivedSuccessfulResponse.getAndSet(true)) {
              requestContext.requestSubmissionToResponseHandlingTime = LatencyUtils.getLatencyInMS(timestampBeforeSendingRequest);

              valueFuture.complete(null);
            }
          } else {
            try {
              metadata.receivedResponseFromInstance(replica, currentVersion, partitionId, HttpStatus.S_200_OK);
              if (!receivedSuccessfulResponse.getAndSet(true)) {
                requestContext.requestSubmissionToResponseHandlingTime = LatencyUtils.getLatencyInMS(timestampBeforeSendingRequest);
                CompressionStrategy compressionStrategy = response.getCompressionStrategy();
                long timestampBeforeDecompression = System.nanoTime();
                ByteBuffer data = decompressRecord(compressionStrategy, ByteBuffer.wrap(response.getBody()));
                requestContext.decompressionTime = LatencyUtils.getLatencyInMS(timestampBeforeDecompression);
                long timestampBeforeDeserialization = System.nanoTime();
                RecordDeserializer<V> deserializer = getDataRecordDeserializer(response.getSchemaId());
                V value = tryToDeserialize(deserializer, data, response.getSchemaId(), key);
                requestContext.responseDeserializationTime = LatencyUtils.getLatencyInMS(timestampBeforeDeserialization);

                valueFuture.complete(value);
              }
            } catch (Exception e) {
              if (!valueFuture.isDone()) {
                valueFuture.completeExceptionally(e);
              }
            }
          }
          return null;
        }, deserializationExecutor);
      } catch (Exception e) {
        LOGGER.error("Received exception while sending request to replica: " + replica, e);
        metadata.receivedResponseFromInstance(replica, currentVersion, partitionId, HttpStatus.S_503_SERVICE_UNAVAILABLE);
      }
    }
    if (transportFutures.isEmpty()) {
      // No request has been sent out.
      valueFuture.completeExceptionally(new VeniceClientException("No available replica for store: "
          + getStoreName() + ", version: " + currentVersion + " and partition: " + partitionId));
      //TODO: metrics?
    } else {
      /**
       * The following handler will be triggered if none of the queries for the same request succeeds.
       */
      CompletableFuture.allOf(transportFutures.toArray(new CompletableFuture[transportFutures.size()])).exceptionally(throwable -> {
        requestContext.requestSubmissionToResponseHandlingTime = LatencyUtils.getLatencyInMS(timestampBeforeSendingRequest);

        valueFuture.completeExceptionally(throwable);
        return null;
      });
    }

    return valueFuture;
  }

  protected RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    Schema readerSchema = metadata.getLatestValueSchema();
    if (null == readerSchema) {
      throw new VeniceClientException("Failed to get latest value schema for store: " + metadata.getStoreName());
    }
    Schema writerSchema = metadata.getValueSchema(schemaId);
    if (null == writerSchema) {
      throw new VeniceClientException("Failed to get writer schema with id: " + schemaId + " from store: " + metadata.getStoreName());
    }
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
  }

  private <T> T tryToDeserialize(RecordDeserializer<T> dataDeserializer, ByteBuffer data, int writerSchemaId, K key) {
    return AbstractAvroStoreClient.tryToDeserializeWithVerboseLogging(dataDeserializer, data, writerSchemaId, key, keySerializer, metadata, LOGGER);
  }

  // TODO: zstd decompression support
  private static ByteBuffer decompressRecord(CompressionStrategy compressionStrategy, ByteBuffer data) {
    try {
      return CompressorFactory.getCompressor(compressionStrategy).decompress(data);
    } catch (IOException e) {
      throw new VeniceClientException(
          String.format("Unable to decompress the record, compressionStrategy=%d", compressionStrategy.getValue()), e);
    }
  }

  @Override
  public void start() throws VeniceClientException {
  }

  @Override
  public void close() {
  }

  @Override
  public String getStoreName() {
    return metadata.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return metadata.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return metadata.getLatestValueSchema();
  }
}
