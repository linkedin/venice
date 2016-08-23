package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.serializer.AvroGenericSerializer;
import com.linkedin.venice.client.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.client.serializer.RecordSerializer;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import javax.validation.constraints.NotNull;
import java.util.Base64;
import java.util.concurrent.Future;

public abstract class AbstractAvroStoreClient<V> implements AvroGenericStoreClient<V>, DeserializerFetcher<V> {
  public static final String STORAGE_TYPE = "storage";
  public static final String B64_FORMAT = "?f=b64";

  /** Encoder to encode store key object */
  private final Base64.Encoder encoder = Base64.getUrlEncoder();
  /** Used to communicate with Venice backend to retrieve necessary store schemas */
  private final SchemaReader schemaReader;
  // Key serializer
  protected RecordSerializer<Object> keySerializer;

  private TransportClient<V> transportClient;
  private String storeName;

  public AbstractAvroStoreClient(TransportClient<V> transportClient,
                                 String storeName,
                                 boolean needSchemaReader) throws VeniceClientException {
    this.transportClient = transportClient;
    this.storeName = storeName;
    if (needSchemaReader) {
      // To avoid cycle dependency, the store client used by SchemaReader
      // won't need to init a new SchemaReader.
      this.schemaReader = new SchemaReader(this.getStoreClientForSchemaReader());
      // init key serializer
      Schema keySchema = this.schemaReader.getKeySchema();
      if (null == keySchema) {
        throw new VeniceClientException("Failed to get key schema for store: " + getStoreName());
      }
      this.keySerializer = AvroSerializerDeserializerFactory.getAvroGenericSerializer(keySchema);
    } else {
      this.schemaReader = null;
    }
    // Set deserializer fetcher for transport client
    transportClient.setDeserializerFetcher(this);
  }

  public String getStoreName() {
    return storeName;
  }

  protected TransportClient<V> getTransportClient() {
    return transportClient;
  }

  @NotNull
  protected SchemaReader getSchemaReader() {
    return schemaReader;
  }

  private String getRequestPathByStoreKey(byte[] key) {
    String b64key = encoder.encodeToString(key);
    return STORAGE_TYPE +
        "/" + storeName +
        "/" + b64key + B64_FORMAT;
  }

  // For testing
  public String getRequestPathByKey(Object key) throws VeniceClientException {
    byte[] serializedKey = keySerializer.serialize(key);
    return getRequestPathByStoreKey(serializedKey);
  }

  @Override
  public Future<V> get(Object key) throws VeniceClientException {
    byte[] serializedKey = keySerializer.serialize(key);
    String requestPath = getRequestPathByStoreKey(serializedKey);

    return transportClient.get(requestPath);
  }

  public Future<byte[]> getRaw(String requestPath) {
    return transportClient.getRaw(requestPath);
  }

  @Override
  public void close() {
    boolean isHttp = transportClient instanceof HttpTransportClient;
    IOUtils.closeQuietly(transportClient);
    if (isHttp) { // TODO make d2client close method idempotent.  d2client re-uses the transport client for the schema reader
      IOUtils.closeQuietly(schemaReader);
    }
  }

  protected abstract AbstractAvroStoreClient<V> getStoreClientForSchemaReader() throws VeniceClientException;

}
