package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.ConfigKeys.DARK_CLUSTER_TARGET_STORES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
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
  public Optional<List<String>> getTargetStores() {
    String stores = getString(DARK_CLUSTER_TARGET_STORES).orElse(null);
    if (stores != null) {
      return Optional.of(com.linkedin.venice.utils.Utils.parseCommaSeparatedStringToList(stores));
    }
    return Optional.empty();
  }

  public UpdateDarkClusterConfigQueryParams setTargetStores(List<String> targetStores) {
    return putStringList(DARK_CLUSTER_TARGET_STORES, targetStores);
  }

  // ***************** above this line are getters and setters *****************
  private UpdateDarkClusterConfigQueryParams putStringList(String name, List<String> value) {
    try {
      return (UpdateDarkClusterConfigQueryParams) add(name, OBJECT_MAPPER.writeValueAsString(value));
    } catch (JsonProcessingException e) {
      throw new VeniceException(e.getMessage());
    }
  }
}
