package com.linkedin.venice.helix;

import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.meta.Version;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Serializer used to convert the data between Store and json.
 */
public class StoreJSONSerializer implements VeniceSerializer<Store> {
    private final ObjectMapper mapper = new ObjectMapper();

    public StoreJSONSerializer() {
        addMixin(Store.class, StoreSerializerMixin.class);
        addMixin(Version.class, VersionSerializerMixin.class);
        addMixin(HybridStoreConfig.class, HybridStoreConfigSerializerMixin.class);
        // Ignore unknown properties
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private void addMixin(Class veniceClass, Class serializerClass) {
        mapper.getDeserializationConfig().addMixInAnnotations(veniceClass, serializerClass);
    }

    @Override
    public byte[] serialize(Store store, String path)
        throws IOException {
        return mapper.writeValueAsBytes(store);
    }

    @Override
    public Store deserialize(byte[] bytes, String path)
        throws IOException {
        return mapper.readValue(bytes, Store.class);
    }

    public ObjectMapper getMapper() {
        return mapper;
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
            @JsonProperty("hybridStoreConfig") HybridStoreConfig hybridStoreConfig) {}
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
}
