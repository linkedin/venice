package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecord;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.IOUtils;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  public static final String TYPE_STORAGE = "storage";
  public static final String B64_FORMAT = "?f=b64";

  /** Encoder to encode store key object */
  private final Base64.Encoder encoder = Base64.getUrlEncoder();

  private final Boolean needSchemaReader;
  /** Used to communicate with Venice backend to retrieve necessary store schemas */
  private SchemaReader schemaReader;
  // Key serializer
  protected RecordSerializer<K> keySerializer;

  private TransportClient transportClient;
  private String storeName;

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
    String b64key = encoder.encodeToString(key);
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

    CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(requestPath);

    // Deserialization
    CompletableFuture<V> valueFuture = transportFuture.handle(
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
        }
    );

    return valueFuture;
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(requestPath);
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
  public CompletableFuture<Map<K, V>> multiGet(Set<K> keys) throws VeniceClientException
  {
    List<K> keyList = new ArrayList<>(keys);
    byte[] multiGetBody = keySerializer.serializeObjects(keyList);
    String requestPath = getStorageRequestPath();

    CompletableFuture<TransportClientResponse> transportFuture = transportClient.post(requestPath, multiGetBody);
    CompletableFuture<Map<K, V>> valueFuture = transportFuture.handle(
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
          RecordDeserializer<MultiGetResponseRecord> deserializer = getMultiGetResponseRecordDeserializer(responseSchemaId);
          Iterable<MultiGetResponseRecord> records = deserializer.deserializeObjects(clientResponse.getBody());
          Map<K, V> resultMap = new HashMap<>();
          for (MultiGetResponseRecord record : records) {
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
        }
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

  private RecordDeserializer<MultiGetResponseRecord> getMultiGetResponseRecordDeserializer(int schemaId) {
    // TODO: get multi-get response write schema from Router
    int currentProtocolVersion = ReadAvroProtocolDefinition.MULTI_GET_RESPONSE.getCurrentProtocolVersion();
    if (currentProtocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + currentProtocolVersion);
    }
    Schema writeSchema = MultiGetResponseRecord.SCHEMA$;
    return AvroSerializerDeserializerFactory.getAvroSpecificDeserializer(writeSchema, MultiGetResponseRecord.class);
  }

  public String toString() {
    return this.getClass().getSimpleName() +
        "(storeName: " + storeName +
        ", transportClient: " + transportClient.toString() + ")";
  }
}
