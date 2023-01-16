package com.linkedin.venice.helix;

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
  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private Class<T> type;

  public VeniceJsonSerializer(Class<T> type) {
    // Ignore unknown properties
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.type = type;
  }

  @Override
  public byte[] serialize(T object, String path) throws IOException {
    // Use pretty JSON format, easy to read.
    byte[] serializedObject = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(object);
    if (serializedObject.length > serializedMapSizeLimit) {
      throw new IOException("Serialized map exceeded the size limit of " + serializedMapSizeLimit + " bytes");
    }
    return serializedObject;
  }

  @Override
  public T deserialize(byte[] bytes, String path) throws IOException {
    return OBJECT_MAPPER.readValue(bytes, type);
  }
}
