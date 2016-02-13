package com.linkedin.venice.helix;

import com.linkedin.venice.meta.OfflinePUshStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreSerializer;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Serializer used to convert the data between Store and json.
 */
public class StoreJSONSerializer implements StoreSerializer {
    private final ObjectMapper mapper = new ObjectMapper();

    public StoreJSONSerializer() {
        mapper.getDeserializationConfig().addMixInAnnotations(Store.class, StoreSerializerMixin.class);
    }

    @Override
    public byte[] serialize(Store store)
        throws IOException {
        return mapper.writeValueAsBytes(store);
    }

    @Override
    public Store deserialize(byte[] bytes)
        throws IOException {
        return mapper.readValue(bytes, Store.class);
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    /**
     * Mixin use to add the annotation to figure out the constructor used by Jackson lib when deserialize.
     */
    public static class StoreSerializerMixin {
        @JsonCreator
        public StoreSerializerMixin(@JsonProperty("name") String name, @JsonProperty("owner") String owner,
            @JsonProperty("createdTime") long createdTime,
            @JsonProperty("persistenceType") PersistenceType persistenceType,
            @JsonProperty("routingStrategy") RoutingStrategy routingStrategy,
            @JsonProperty("readStrategy") ReadStrategy readStrategy,
            @JsonProperty("offLinePushStrategy") OfflinePUshStrategy offlinePushStrategy) {

        }
    }
}
