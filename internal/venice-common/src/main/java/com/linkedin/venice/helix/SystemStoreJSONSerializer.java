package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.SerializableSystemStore;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import java.io.IOException;


public class SystemStoreJSONSerializer extends VeniceJsonSerializer<SerializableSystemStore> {
  public SystemStoreJSONSerializer() {
    super(SerializableSystemStore.class);
  }

  @Override
  protected void configureObjectMapper(ObjectMapper mapper) {
    // Register mixins on this serializer's own ObjectMapper instance
    mapper.addMixIn(ZKStore.class, StoreJSONSerializer.StoreSerializerMixin.class);
    mapper.addMixIn(Version.class, StoreJSONSerializer.VersionSerializerMixin.class);
    mapper.addMixIn(HybridStoreConfig.class, StoreJSONSerializer.HybridStoreConfigSerializerMixin.class);
    mapper.addMixIn(ETLStoreConfig.class, StoreJSONSerializer.ETLStoreConfigSerializerMixin.class);
    mapper.addMixIn(PartitionerConfig.class, StoreJSONSerializer.PartitionerConfigSerializerMixin.class);
    mapper.addMixIn(SerializableSystemStore.class, SystemStoreSerializerMixin.class);
  }

  public static class SystemStoreSerializerMixin {
    @JsonCreator
    public SystemStoreSerializerMixin(
        @JsonProperty("zkSharedStore") ZKStore zkSharedStore,
        @JsonProperty("systemStoreType") VeniceSystemStoreType systemStoreType,
        @JsonProperty("veniceStore") ZKStore veniceStore) {
    }
  }

  @Override
  public byte[] serialize(SerializableSystemStore systemStore, String path) throws IOException {
    return super.serialize(systemStore, path);
  }

  @Override
  public SerializableSystemStore deserialize(byte[] bytes, String path) throws IOException {
    return objectMapper.readValue(bytes, SerializableSystemStore.class);
  }
}
