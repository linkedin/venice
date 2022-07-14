package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public class UpdateStoragePersonaQueryParams extends QueryParams {

  private static final ObjectMapper mapper = ObjectMapperFactory.getInstance();

  public UpdateStoragePersonaQueryParams(Map<String, String> initialParams) { super(initialParams); }

  public UpdateStoragePersonaQueryParams() {
    super();
  }

  public Optional<Set<String>> getOwners() {
    return getStringSet(PERSONA_OWNERS);
  }

  public UpdateStoragePersonaQueryParams setName(String name) {
    return (UpdateStoragePersonaQueryParams) add(NAME, name);
  }

  public Optional<String> getName() {
    return getString(NAME);
  }

  public UpdateStoragePersonaQueryParams setOwners(Set<String> owners) {
    return (UpdateStoragePersonaQueryParams) putStringSet(PERSONA_OWNERS, owners);
  }

  public Optional<Set<String>> getStoresToEnforce() {
    return getStringSet(PERSONA_STORES);
  }

  public UpdateStoragePersonaQueryParams setStoresToEnforce(Set<String> stores) {
    return (UpdateStoragePersonaQueryParams) putStringSet(PERSONA_STORES, stores);
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

  private UpdateStoragePersonaQueryParams putLong(String name, long value) {
    return (UpdateStoragePersonaQueryParams) add(name, value);
  }


}
