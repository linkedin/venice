package com.linkedin.venice.helix;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ViewConfig;
import java.io.IOException;


/**
 * Serializer used to convert a single {@link Version} to and from JSON for storage in its own ZK znode
 * at /<cluster>/Stores/<storeName>/versions/<versionNumber>.
 *
 * Reuses the nested-type mixins declared on {@link StoreJSONSerializer} so a version znode and the version elements
 * embedded in a legacy store znode share identical wire format.
 */
public class VersionJSONSerializer extends VeniceJsonSerializer<Version> {
  public VersionJSONSerializer() {
    super(Version.class);
  }

  @Override
  protected void configureObjectMapper(ObjectMapper mapper) {
    mapper.addMixIn(Version.class, StoreJSONSerializer.VersionSerializerMixin.class);
    mapper.addMixIn(HybridStoreConfig.class, StoreJSONSerializer.HybridStoreConfigSerializerMixin.class);
    mapper.addMixIn(PartitionerConfig.class, StoreJSONSerializer.PartitionerConfigSerializerMixin.class);
    mapper.addMixIn(ViewConfig.class, StoreJSONSerializer.ViewConfigSerializerMixin.class);
  }

  @Override
  public byte[] serialize(Version object, String path) throws IOException {
    if (!(object instanceof VersionImpl)) {
      throw new VeniceException("This serializer only supports VersionImpl type for json serialization");
    }
    return super.serialize(object, path);
  }

  @Override
  public Version deserialize(byte[] bytes, String path) throws IOException {
    return objectMapper.readValue(bytes, VersionImpl.class);
  }
}
