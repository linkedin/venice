package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.ConfigKeys.STORES_TO_REPLICATE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class UpdateDarkClusterConfigQueryParams extends QueryParams {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  public UpdateDarkClusterConfigQueryParams(Map<String, String> initialParams) {
    super(initialParams);
  }

  public UpdateDarkClusterConfigQueryParams() {
    super();
  }

  // ***************** below this line are getters and setters *****************
  public Optional<List<String>> getStoresToReplicate() {
    if (params.get(STORES_TO_REPLICATE) == null) {
      return Optional.empty();
    }

    return getStringList(STORES_TO_REPLICATE);
  }

  public UpdateDarkClusterConfigQueryParams setStoresToReplicate(List<String> storesToReplicate) {
    return putStringList(STORES_TO_REPLICATE, storesToReplicate);
  }

  // ***************** above this line are getters and setters *****************
  private UpdateDarkClusterConfigQueryParams putStringList(String name, List<String> value) {
    try {
      return (UpdateDarkClusterConfigQueryParams) add(name, OBJECT_MAPPER.writeValueAsString(value));
    } catch (JsonProcessingException e) {
      throw new VeniceException(e.getMessage());
    }
  }

  public Optional<List<String>> getStringList(String name) {
    if (!params.containsKey(name)) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(OBJECT_MAPPER.readValue(params.get(name), List.class));
      } catch (IOException e) {
        throw new VeniceException(e);
      }
    }
  }
}
