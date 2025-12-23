package com.linkedin.venice.helix;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;


public class VeniceJsonSerializer<T> implements VeniceSerializer<T> {
  /**
   * ZK has a max size limit of 0xfffff bytes or just under 1 MB of data per znode specified by jute.maxbuffer,
   * will throw exception if the serialized map exceeds this limit.
   */
  private final static int serializedMapSizeLimit = 0xfffff;

  /**
   * Instance-level ObjectMapper to avoid shared state and race conditions.
   * Each serializer gets its own configured ObjectMapper instance.
   */
  protected final ObjectMapper objectMapper;
  private Class<T> type;
  private TypeReference<T> typeReference;

  public VeniceJsonSerializer(Class<T> type) {
    this.objectMapper = createObjectMapper();
    this.type = type;
  }

  public VeniceJsonSerializer(TypeReference<T> typeReference) {
    this.objectMapper = createObjectMapper();
    this.typeReference = typeReference;
  }

  /**
   * Creates a new ObjectMapper instance with default Venice configuration.
   * This is final to prevent it being called from constructor in an unsafe way.
   * Subclasses should use configureObjectMapper() to customize the ObjectMapper.
   */
  protected final ObjectMapper createObjectMapper() {
    ObjectMapper mapper = ObjectMapperFactory.getInstance().copy();
    // Ignore unknown properties
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Allow subclasses to configure the mapper
    configureObjectMapper(mapper);

    return mapper;
  }

  /**
   * Configure the ObjectMapper with custom settings.
   * Subclasses should override this method instead of createObjectMapper().
   * @param mapper The ObjectMapper to configure
   */
  protected void configureObjectMapper(ObjectMapper mapper) {
    // Default implementation does nothing
  }

  /**
   * Get the ObjectMapper instance for this serializer.
   * Protected to allow subclasses to access it for configuration.
   */
  protected ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  @Override
  public byte[] serialize(T object, String path) throws IOException {
    // Use pretty JSON format, easy to read.
    byte[] serializedObject = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(object);
    if (serializedObject.length > serializedMapSizeLimit) {
      throw new IOException("Serialized map exceeded the size limit of " + serializedMapSizeLimit + " bytes");
    }
    return serializedObject;
  }

  @Override
  public T deserialize(byte[] bytes, String path) throws IOException {
    if (typeReference != null) {
      return objectMapper.readValue(bytes, typeReference);
    }
    return objectMapper.readValue(bytes, type);
  }
}
