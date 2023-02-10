package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ZKStore;
import java.io.IOException;
import java.util.Map;


/**
 * Serializer used to convert the data between Store and json.
 */
public class StoreJSONSerializer extends VeniceJsonSerializer<Store> {
  public StoreJSONSerializer() {
    super(Store.class);
    addMixin(ZKStore.class, StoreSerializerMixin.class);
    addMixin(Version.class, VersionSerializerMixin.class);
    addMixin(HybridStoreConfig.class, HybridStoreConfigSerializerMixin.class);
    addMixin(ETLStoreConfig.class, ETLStoreConfigSerializerMixin.class);
    addMixin(PartitionerConfig.class, PartitionerConfigSerializerMixin.class);
    addMixin(ViewConfig.class, ViewConfigSerializerMixin.class);
  }

  private void addMixin(Class veniceClass, Class serializerClass) {
    OBJECT_MAPPER.addMixIn(veniceClass, serializerClass);
  }

  /**
   * Mixin used to add the annotation to figure out the constructor used by Jackson lib when deserialize the store.
   */
  public static class StoreSerializerMixin {
    @JsonCreator
    public StoreSerializerMixin(
        @JsonProperty("name") String name,
        @JsonProperty("owner") String owner,
        @JsonProperty("createdTime") long createdTime,
        @JsonProperty("persistenceType") PersistenceType persistenceType,
        @JsonProperty("routingStrategy") RoutingStrategy routingStrategy,
        @JsonProperty("readStrategy") ReadStrategy readStrategy,
        @JsonProperty("offLinePushStrategy") OfflinePushStrategy offlinePushStrategy,
        @JsonProperty("currentVersion") int currentVersion,
        @JsonProperty("storageQuotaInByte") long storageQuotaInByte,
        @JsonProperty("readQuotaInCU") long readQuotaInCU,
        @JsonProperty("hybridStoreConfig") HybridStoreConfig hybridStoreConfig,
        @JsonProperty("partitionerConfig") PartitionerConfig partitionerConfig,
        @JsonProperty("replicationFactor") int replicationFactor) {
    }
  }

  /**
   * Mixin used to add the annotation to figure out the constructor used by Jackson lib when deserialize the version
   */
  public static class VersionSerializerMixin {
    @JsonCreator
    public VersionSerializerMixin(
        @JsonProperty("storeName") String storeName,
        @JsonProperty("number") int number,
        @JsonProperty("createdTime") long createdTime) {
    }
  }

  /**
   * Mixin used to add the annotation to figure out the constructor used by Jackson lib when deserialize the version
   */
  public static class HybridStoreConfigSerializerMixin {
    @JsonCreator
    public HybridStoreConfigSerializerMixin(
        @JsonProperty("rewindTimeInSeconds") long rewindTimeInSeconds,
        @JsonProperty("offsetLagThresholdToGoOnline") long offsetLagThresholdToGoOnline) {
    }
  }

  public static class ETLStoreConfigSerializerMixin {
    @JsonCreator
    public ETLStoreConfigSerializerMixin(
        @JsonProperty("etledUserProxyAccount") String etledUserProxyAccount,
        @JsonProperty("regularVersionETLEnabled") boolean regularVersionETLEnabled,
        @JsonProperty("futureVersionETLEnabled") boolean futureVersionETLEnabled) {
    }
  }

  public static class ViewConfigSerializerMixin {
    @JsonCreator
    public ViewConfigSerializerMixin(
        @JsonProperty("viewClassName") String viewClassName,
        @JsonProperty("viewParameters") Map<String, String> viewParameters) {
    }
  }

  public static class PartitionerConfigSerializerMixin {
    @JsonCreator
    public PartitionerConfigSerializerMixin(
        @JsonProperty("partitionerClass") String partitionerClass,
        @JsonProperty("partitionerParams") Map<String, String> partitionerParams,
        @JsonProperty("amplificationFactor") int amplificationFactor) {
    }
  }

  @Override
  public byte[] serialize(Store object, String path) throws IOException {
    /**
     * This function will only serialize {@link ZKStore}.
     */
    if (!(object instanceof ZKStore)) {
      throw new VeniceException("This serializer only supports ZKStore type for json serialization");
    }
    return super.serialize(object, path);
  }

  @Override
  public Store deserialize(byte[] bytes, String path) throws IOException {
    /**
     * This function will only deserialize into {@link ZKStore} implementation.
     */
    return OBJECT_MAPPER.readValue(bytes, ZKStore.class);
  }
}
