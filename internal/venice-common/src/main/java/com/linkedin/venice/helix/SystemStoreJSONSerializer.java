package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    addMixin(ZKStore.class, StoreJSONSerializer.StoreSerializerMixin.class);
    addMixin(Version.class, StoreJSONSerializer.VersionSerializerMixin.class);
    addMixin(HybridStoreConfig.class, StoreJSONSerializer.HybridStoreConfigSerializerMixin.class);
    addMixin(ETLStoreConfig.class, StoreJSONSerializer.ETLStoreConfigSerializerMixin.class);
    addMixin(PartitionerConfig.class, StoreJSONSerializer.PartitionerConfigSerializerMixin.class);
    addMixin(SerializableSystemStore.class, SystemStoreSerializerMixin.class);
  }

  public static class SystemStoreSerializerMixin {
    @JsonCreator
    public SystemStoreSerializerMixin(
        @JsonProperty("zkSharedStore") ZKStore zkSharedStore,
        @JsonProperty("systemStoreType") VeniceSystemStoreType systemStoreType,
        @JsonProperty("veniceStore") ZKStore veniceStore) {
    }
  }

  private void addMixin(Class veniceClass, Class serializerClass) {
    OBJECT_MAPPER.addMixIn(veniceClass, serializerClass);
  }

  @Override
  public byte[] serialize(SerializableSystemStore systemStore, String path) throws IOException {
    return super.serialize(systemStore, path);
  }

  @Override
  public SerializableSystemStore deserialize(byte[] bytes, String path) throws IOException {
    return OBJECT_MAPPER.readValue(bytes, SerializableSystemStore.class);
  }
}
