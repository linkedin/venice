package com.linkedin.venice.persona;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import java.util.Set;


/**
 * Serializer used to convert the data between {@link Persona} and json.
 */
public class StoragePersonaJSONSerializer extends VeniceJsonSerializer<StoragePersona> {
  public StoragePersonaJSONSerializer() {
    super(StoragePersona.class);
    OBJECT_MAPPER.addMixIn(StoragePersona.class, StoragePersonaSerializerMixin.class);
  }

  /** This class annotates the constructor for {@link StoragePersona} and serves as a property-based creator.
   * See the Jackson documentation for more information about property-based creators. */
  public static class StoragePersonaSerializerMixin {
    @JsonCreator
    public StoragePersonaSerializerMixin(
        @JsonProperty("name") String name,
        @JsonProperty("quotaNumber") long quotaNumber,
        @JsonProperty("storesToEnforce") Set<String> storesToEnforce,
        @JsonProperty("owners") Set<String> owners) {
    }
  }
}
