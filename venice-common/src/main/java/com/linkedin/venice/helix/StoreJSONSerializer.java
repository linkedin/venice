package com.linkedin.venice.helix;

import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Serializer used to convert the data between Store and json.
 */
public class StoreJSONSerializer extends VeniceJsonSerializer<Store> {

    public StoreJSONSerializer() {
        super(Store.class);
        addMixin(Store.class, StoreSerializerMixin.class);
        addMixin(Version.class, VersionSerializerMixin.class);
        addMixin(HybridStoreConfig.class, HybridStoreConfigSerializerMixin.class);
        addMixin(ETLStoreConfig.class, ETLStoreConfigSerializerMixin.class);
        addMixin(PartitionerConfig.class, PartitionerConfigSerializerMixin.class);
    }

    private void addMixin(Class veniceClass, Class serializerClass) {
        mapper.getDeserializationConfig().addMixInAnnotations(veniceClass, serializerClass);
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
            @JsonProperty("partitionerConfig") PartitionerConfig partitionerConfig) {}
    }

    /**
     * Mixin used to add the annotation to figure out the constructor used by Jackson lib when deserialize the version
     */
    public static class VersionSerializerMixin {
        @JsonCreator
        public VersionSerializerMixin(
            @JsonProperty("storeName") String storeName,
            @JsonProperty("number") int number,
            @JsonProperty("createdTime") long createdTime) {}
    }

    /**
     * Mixin used to add the annotation to figure out the constructor used by Jackson lib when deserialize the version
     */
    public static class HybridStoreConfigSerializerMixin {
        @JsonCreator
        public HybridStoreConfigSerializerMixin(
            @JsonProperty("rewindTimeInSeconds") long rewindTimeInSeconds,
            @JsonProperty("offsetLagThresholdToGoOnline") long offsetLagThresholdToGoOnline) {}
    }

    public static class ETLStoreConfigSerializerMixin {
        @JsonCreator
        public ETLStoreConfigSerializerMixin(
            @JsonProperty("etledUserProxyAccount") String etledUserProxyAccount,
            @JsonProperty("regularVersionETLEnabled") boolean regularVersionETLEnabled,
            @JsonProperty("futureVersionETLEnabled") boolean futureVersionETLEnabled) {}
    }

    public static class PartitionerConfigSerializerMixin {
        @JsonCreator
        public PartitionerConfigSerializerMixin(
            @JsonProperty("partitionerClass") String partitionerClass,
            @JsonProperty("partitionerParams") Map<String, String> partitionerParams,
            @JsonProperty("amplificationFactor") int amplificationFactor) {}
    }
}
