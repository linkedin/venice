package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public class UpdateStoragePersonaQueryParams extends QueryParams {

  private static final ObjectMapper mapper = ObjectMapperFactory.getInstance();

  public UpdateStoragePersonaQueryParams() {
    super();
  }

  public Optional<Set<String>> getOwners() {
    return getStringSet(PERSONA_OWNERS);
  }

  public UpdateStoragePersonaQueryParams setOwners(Set<String> owners) {
    return putStringSet(PERSONA_OWNERS, owners);
  }

  public Optional<Set<String>> getStoresToEnforce() {
    return getStringSet(PERSONA_STORES);
  }

  public UpdateStoragePersonaQueryParams setStoresToEnforce(Set<String> stores) {
    return putStringSet(PERSONA_STORES, stores);
  }

  public Optional<Long> getQuota() {
    return getLong(PERSONA_QUOTA);
  }

  public UpdateStoragePersonaQueryParams setQuota(long quota) {
    return putLong(PERSONA_QUOTA, quota);
  }

  public void applyParams(StoragePersona persona) {
    getOwners().ifPresent(persona::setOwners);
    getStoresToEnforce().ifPresent(persona::setStoresToEnforce);
    getQuota().ifPresent(persona::setQuotaNumber);
  }

  private Optional<Long> getLong(String name) {
    return Optional.ofNullable(params.get(name)).map(Long::valueOf);
  }

  private Optional<Set<String>> getStringSet(String name) {
    if (!params.containsKey(name)) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(mapper.readValue(params.get(name), Set.class));
      } catch (IOException e) {
        throw new VeniceException(e);
      }
    }
  }

  private UpdateStoragePersonaQueryParams putStringSet(String name, Set<String> value) {
    try {
      return (UpdateStoragePersonaQueryParams) add(
          name,
          mapper.writeValueAsString(value)
      );
    } catch (JsonProcessingException e) {
      throw new VeniceException(e);
    }
  }

  private UpdateStoragePersonaQueryParams putLong(String name, long value) {
    return (UpdateStoragePersonaQueryParams) add(name, value);
  }


}
