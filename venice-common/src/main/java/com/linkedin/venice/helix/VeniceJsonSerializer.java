package com.linkedin.venice.helix;

import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;


public class VeniceJsonSerializer<T> implements VeniceSerializer<T> {
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
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public T deserialize(byte[] bytes, String path)
      throws IOException {
    return mapper.readValue(bytes, type);
  }
}
