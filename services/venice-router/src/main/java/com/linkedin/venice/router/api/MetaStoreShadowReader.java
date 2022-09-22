package com.linkedin.venice.router.api;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreValueSchema;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class serves as the shadow read handler of the meta system store.
 *
 */
public class MetaStoreShadowReader {
  private static final Logger LOGGER = LogManager.getLogger(MetaStoreShadowReader.class);
  private final ReadOnlySchemaRepository schemaRepo;
  private final RecordDeserializer<StoreMetaKey> keyDeserializer =
      FastSerializerDeserializerFactory.getAvroSpecificDeserializer(StoreMetaKey.class);
  private final int metaSystemStoreSchemaId =
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
  private final RecordSerializer<StoreMetaValue> valueSerializer = FastSerializerDeserializerFactory
      .getAvroGenericSerializer(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema());
  private final RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

  public MetaStoreShadowReader(ReadOnlySchemaRepository readOnlySchemaRepository) {
    this.schemaRepo = readOnlySchemaRepository;
  }

  /**
   * @return True only if a meta store query request is indicated not found in the provided response returned from the
   * storage replica.
   */
  public boolean shouldPerformShadowRead(VenicePath path, FullHttpResponse response) {
    // Meta Store read should be single get.
    if (!(!path.isStreamingRequest() && path.getRequestType().equals(RequestType.SINGLE_GET))) {
      return false;
    }
    // Only process not existing value for meta system store.
    return VeniceSystemStoreType.META_STORE.isSystemStore(path.getStoreName())
        && response.status().equals(HttpResponseStatus.NOT_FOUND);
  }

  /**
   * This method performs shadow read for meta system store request in Router.
   * @param path Request path for Meta Store metadata.
   * @param originalResponse Original response retrieved from storage replica.
   * @return Updated {@link FullHttpResponse} with filled in content from metadata repository.
   */
  public FullHttpResponse shadowReadMetaStore(VenicePath path, FullHttpResponse originalResponse) {
    try {
      StoreMetaValue value = new StoreMetaValue();
      String metaStoreName = path.getStoreName();
      String storeName = VeniceSystemStoreType.META_STORE.extractRegularStoreName(metaStoreName);
      ByteBuffer keyBuffer = path.getPartitionKey().getKeyBuffer();
      StoreMetaKey storeMetaKey = keyDeserializer.deserialize(keyBuffer);
      MetaStoreDataType metaStoreDataType = MetaStoreDataType.valueOf(storeMetaKey.metadataType);
      switch (metaStoreDataType) {
        case STORE_PROPERTIES:
          // TODO: Add support to generate store properties StoreMetaValue response in the next step.
          return originalResponse;
        case STORE_KEY_SCHEMAS:
          StoreKeySchemas storeKeySchemas = new StoreKeySchemas();
          SchemaEntry keySchemaEntry = schemaRepo.getKeySchema(storeName);
          storeKeySchemas.keySchemaMap =
              Collections.singletonMap(Integer.toString(keySchemaEntry.getId()), keySchemaEntry.getSchemaStr());
          value.storeKeySchemas = storeKeySchemas;
          break;
        case STORE_VALUE_SCHEMA:
          StoreValueSchema storeValueSchema = new StoreValueSchema();
          int valueSchemaID = Integer.parseInt(storeMetaKey.keyStrings.get(1).toString());
          SchemaEntry valueSchemaEntry = schemaRepo.getValueSchema(storeName, valueSchemaID);
          storeValueSchema.valueSchema = valueSchemaEntry.getSchemaStr();
          value.storeValueSchema = storeValueSchema;
          break;
        case STORE_VALUE_SCHEMAS:
          StoreValueSchemas storeValueSchemas = new StoreValueSchemas();
          storeValueSchemas.valueSchemaMap = schemaRepo.getValueSchemas(storeName)
              .stream()
              .collect(Collectors.toMap(entry -> Integer.toString(entry.getId()), SchemaEntry::getSchemaStr));
          value.storeValueSchemas = storeValueSchemas;
          break;
        default:
          // Defensive coding here. Other MetaStoreDataType is not expected to be queried via store client.
          return originalResponse;
      }
      byte[] data = valueSerializer.serialize(value);
      return buildUpdatedResponse(data);
    } catch (Exception e) {
      // Use redundant exception filter to make sure no log spamming happen on Router.
      if (!filter.isRedundantException(path.getStoreName(), e)) {
        LOGGER.error("Got exception when performing meta store shadow read {} ", path.getStoreName(), e);
      }
      /**
       * In case shadow read failed, we return the original Http response. In this case, it will still fail on
       * Da Vinci clients, but it won't impact router functionality to serve other tenants.
       */
      return originalResponse;
    }
  }

  private FullHttpResponse buildUpdatedResponse(byte[] data) {
    ByteBuf body = Unpooled.wrappedBuffer(data, 0, data.length);
    FullHttpResponse updatedResponse = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK, body);
    updatedResponse.headers().set(CONTENT_TYPE, HttpConstants.TEXT_PLAIN);
    updatedResponse.headers().set(CONTENT_LENGTH, body.readableBytes());
    updatedResponse.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP);
    updatedResponse.headers().set(HttpConstants.VENICE_SCHEMA_ID, metaSystemStoreSchemaId);
    updatedResponse.headers().set(HttpConstants.VENICE_REQUEST_RCU, 1);
    return updatedResponse;
  }
}
