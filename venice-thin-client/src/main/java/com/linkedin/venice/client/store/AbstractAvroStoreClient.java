package com.linkedin.venice.client.store;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.EncodingUtils;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.commons.io.IOUtils;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  public static final String TYPE_STORAGE = "storage";
  public static final String B64_FORMAT = "?f=b64";

  private static final Map<String, String> GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> MULTI_GET_HEADER_MAP = new HashMap<>();
  static {
    /**
     * Hard-code API version of single-get and multi-get to be '1'.
     * If the header varies request by request, Venice client needs to create a map per request.
     */
    GET_HEADER_MAP.put(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
    MULTI_GET_HEADER_MAP.put(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
  }

  private final CompletableFuture<Map<K, V>> COMPLETABLE_FUTURE_FOR_EMPTY_KEY_IN_BATCH_GET = CompletableFuture.completedFuture(new HashMap<>());

  private final Boolean needSchemaReader;
  /** Used to communicate with Venice backend to retrieve necessary store schemas */
  private SchemaReader schemaReader;
  // Key serializer
  protected RecordSerializer<K> keySerializer;
  // Multi-get request serializer
  protected RecordSerializer<ByteBuffer> multiGetRequestSerializer;

  private TransportClient transportClient;
  private String storeName;

  /**
   * Here is the details about the deadlock issue if deserialization logic is executed in the same R2 callback thread:
   * 1. A bunch of regular get requests are sent to Venice backend at the same time;
   * 2. All those requests return almost at the same time;
   * 3. All those requests will be blocked by the {@link SchemaReader#fetchValueSchema(int)}
   *    if the value schema is not in local cache;
   * 4. At this moment, each request will occupy one internal R2 callback thread, and the R2 callback threads will be
   *    exhausted if there are a lot of simultaneous regular get requests;
   * 5. Since all the R2 callback threads are blocked by the schema request, then there is no more R2 callback thread could
   *    handle the callback of the schema request;
   * 6. The deadlock issue happens;
   *
   * Loading all the value schemas during start won't solve this problem since the value schema could involve when store
   * client is running.
   * So we have to use a different set of threads in {@link SchemaReader} to avoid this issue.
   * Also, we don't want to use the default thread pool: {@link CompletableFuture#useCommonPool} since it is being shared,
   * and the deserialization could be blocked by the logic not belonging to Venice Client.
   **/
  private static final int DESERIALIZATION_THREAD_NUM;
  private static final Executor DESERIALIZATION_EXECUTOR;

  static {
    // Half of process number of threads should be good enough
    int halfOfAvailableProcessors = Runtime.getRuntime().availableProcessors() / 2;
    DESERIALIZATION_THREAD_NUM = halfOfAvailableProcessors > 0 ? halfOfAvailableProcessors : 1;
    DESERIALIZATION_EXECUTOR = Executors.newFixedThreadPool(DESERIALIZATION_THREAD_NUM,
        new DaemonThreadFactory("Venice-Store-Deserialization"));
  }

  public AbstractAvroStoreClient(TransportClient transportClient,
                                 String storeName,
                                 boolean needSchemaReader) {
    this.transportClient = transportClient;
    this.storeName = storeName;
    this.needSchemaReader = needSchemaReader;
  }

  @Override
  public String getStoreName() {
    return storeName;
  }

  protected TransportClient getTransportClient() {
    return transportClient;
  }

  @NotNull
  protected SchemaReader getSchemaReader() {
    return schemaReader;
  }

  private String getStorageRequestPathForSingleKey(byte[] key) {
    String b64key = EncodingUtils.base64EncodeToString(key);
    return getStorageRequestPath() +
        "/" + b64key + B64_FORMAT;
  }

  private String getStorageRequestPath() {
    return TYPE_STORAGE + "/" + storeName;
  }

  // For testing
  public String getRequestPathByKey(K key) throws VeniceClientException {
    byte[] serializedKey = keySerializer.serialize(key);
    return getStorageRequestPathForSingleKey(serializedKey);
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    byte[] serializedKey = keySerializer.serialize(key);
    String requestPath = getStorageRequestPathForSingleKey(serializedKey);

    CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(requestPath, GET_HEADER_MAP);

    // Deserialization
    CompletableFuture<V> valueFuture = transportFuture.handleAsync(
        (clientResponse, throwable) -> {
          if (null != throwable) {
            handleStoreExceptionInternally(throwable);
          }
          if (null == clientResponse) {
            // Doesn't exist
            return null;
          }
          if (!clientResponse.isSchemaIdValid()) {
            throw new VeniceClientException("No valid schema id received for single-get request!");
          }
          RecordDeserializer<V> deserializer = getDataRecordDeserializer(clientResponse.getSchemaId());
          return deserializer.deserialize(clientResponse.getBody());
        },
        DESERIALIZATION_EXECUTOR
    );

    return valueFuture;
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(requestPath);
    // No need to use another thread pool since there is no deserialization logic here.
    CompletableFuture<byte[]> valueFuture = transportFuture.handle(
        (clientResponse, throwable) -> {
          if (null != throwable) {
            handleStoreExceptionInternally(throwable);
          }
          if (null == clientResponse) {
            // Doesn't exist
            return null;
          }
          return clientResponse.getBody();
        }
    );
    return valueFuture;
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException
  {
    if (keys.isEmpty()) {
      return COMPLETABLE_FUTURE_FOR_EMPTY_KEY_IN_BATCH_GET;
    }
    List<K> keyList = new ArrayList<>(keys);
    List<ByteBuffer> serializedKeyList = new ArrayList<>();
    keyList.stream().forEach( key -> serializedKeyList.add(ByteBuffer.wrap(keySerializer.serialize(key))) );
    byte[] multiGetBody = multiGetRequestSerializer.serializeObjects(serializedKeyList);
    String requestPath = getStorageRequestPath();

    CompletableFuture<TransportClientResponse> transportFuture = transportClient.post(requestPath, MULTI_GET_HEADER_MAP,
        multiGetBody);
    CompletableFuture<Map<K, V>> valueFuture = transportFuture.handleAsync(
        (clientResponse, throwable) -> {
          if (null != throwable) {
            handleStoreExceptionInternally(throwable);
          }
          if (null == clientResponse) {
            // Even all the keys don't exist in Venice, multi-get should receive empty result instead of 'null'
            throw new VeniceClientException("TransportClient should not return null for multi-get request");
          }
          if (!clientResponse.isSchemaIdValid()) {
            throw new VeniceClientException("No valid schema id received for multi-get request!");
          }
          int responseSchemaId = clientResponse.getSchemaId();
          RecordDeserializer<MultiGetResponseRecordV1> deserializer = getMultiGetResponseRecordDeserializer(responseSchemaId);
          Iterable<MultiGetResponseRecordV1> records = deserializer.deserializeObjects(clientResponse.getBody());
          Map<K, V> resultMap = new HashMap<>();
          for (MultiGetResponseRecordV1 record : records) {
            int keyIdx = record.keyIndex;
            if (keyIdx >= keyList.size() || keyIdx < 0) {
              throw new VeniceClientException("Key index: " + keyIdx + " doesn't have a corresponding key");
            }
            int recordSchemaId = record.schemaId;
            byte[] serializedData = record.value.array();
            RecordDeserializer<V> dataDeserializer = getDataRecordDeserializer(recordSchemaId);
            V value = dataDeserializer.deserialize(serializedData);
            resultMap.put(keyList.get(keyIdx), value);
          }

          return resultMap;
        },
        DESERIALIZATION_EXECUTOR
    );


    return valueFuture;
  }

  @Override
  public void start() throws VeniceClientException {
    if (needSchemaReader) {
      //TODO: remove the 'instanceof' statement once HttpClient got refactored.
      if (transportClient instanceof D2TransportClient) {
        this.schemaReader = new SchemaReader(this);
      } else {
        this.schemaReader = new SchemaReader(this.getStoreClientForSchemaReader());
      }

      // init key serializer
      this.keySerializer =
        AvroSerializerDeserializerFactory.getAvroGenericSerializer(schemaReader.getKeySchema());
      // init multi-get request serializer
      this.multiGetRequestSerializer = AvroSerializerDeserializerFactory.getAvroGenericSerializer(
          ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
    } else {
      this.schemaReader = null;
    }
  }

  @Override
  public void close() {
    boolean isHttp = transportClient instanceof HttpTransportClient;
    IOUtils.closeQuietly(transportClient);
    if (isHttp) { // TODO make d2client close method idempotent.  d2client re-uses the transport client for the schema reader
      IOUtils.closeQuietly(schemaReader);
    }
  }

  protected abstract AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader();

  public abstract RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException;

  private RecordDeserializer<MultiGetResponseRecordV1> getMultiGetResponseRecordDeserializer(int schemaId) {
    // TODO: get multi-get response write schema from Router
    int protocolVersion = ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
    return AvroSerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);
  }

  public String toString() {
    return this.getClass().getSimpleName() +
        "(storeName: " + storeName +
        ", transportClient: " + transportClient.toString() + ")";
  }
}
