package com.linkedin.venice.helix;

import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;


public class VeniceJsonSerializer<T> implements VeniceSerializer<T> {
  /**
   * ZK has a max size limit of 0xfffff bytes or just under 1 MB of data per znode specified by jute.maxbuffer,
   * will throw exception if the serialized map exceeds this limit.
   */
  private final int serializedMapSizeLimit = 0xfffff;
  protected final ObjectMapper mapper = new ObjectMapper();
  private Class<T> type;

  public VeniceJsonSerializer(Class<T> type) {
    // Ignore unknown properties
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.type = type;
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  @Override
  public byte[] serialize(T object, String path)
      throws IOException {
    // Use pretty JSON format, easy to read.
    byte[] serializedObject = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(object);
    if (serializedObject.length > serializedMapSizeLimit) {
      throw new IOException("Serialized map exceeded the size limit of " + serializedMapSizeLimit + " bytes");
    }
    return serializedObject;
  }

  @Override
  public T deserialize(byte[] bytes, String path)
      throws IOException {
    return mapper.readValue(bytes, type);
  }
}
