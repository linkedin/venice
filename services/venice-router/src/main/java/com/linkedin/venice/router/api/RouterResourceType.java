package com.linkedin.venice.router.api;

import java.util.HashMap;
import java.util.Map;


public enum RouterResourceType {
  TYPE_LEADER_CONTROLLER("leader_controller"), @Deprecated
  TYPE_LEADER_CONTROLLER_LEGACY("master_controller"), TYPE_KEY_SCHEMA("key_schema"), TYPE_VALUE_SCHEMA("value_schema"),
  TYPE_LATEST_VALUE_SCHEMA("latest_value_schema"), TYPE_GET_UPDATE_SCHEMA("update_schema"),
  TYPE_CLUSTER_DISCOVERY("discover_cluster"), TYPE_REQUEST_TOPIC("request_topic"),
  TYPE_STREAM_HYBRID_STORE_QUOTA("stream_hybrid_store_quota"),
  TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA("stream_reprocessing_hybrid_store_quota"),
  TYPE_STORE_STATE("store_state"), TYPE_PUSH_STATUS("push_status"), TYPE_STORAGE("storage"), TYPE_COMPUTE("compute"),
  TYPE_ADMIN("admin"), TYPE_RESOURCE_STATE("resource_state"), TYPE_INVALID("invalid");

  private static final RouterResourceType[] RESOURCE_TYPE_VALUES = RouterResourceType.values();
  private static final Map<String, RouterResourceType> ROUTER_RESOURCE_TYPE_MAP = getResourceTypeMap();

  private final String typeName;

  RouterResourceType(String typeName) {
    this.typeName = typeName;
  }

  public static RouterResourceType getTypeResourceType(String typeName) {
    RouterResourceType type = ROUTER_RESOURCE_TYPE_MAP.get(typeName);
    if (type == null) {
      return TYPE_INVALID;
    }
    return type;
  }

  @Override
  public String toString() {
    return typeName;
  }

  private static Map<String, RouterResourceType> getResourceTypeMap() {
    Map<String, RouterResourceType> routerResourceTypeMap = new HashMap<>();
    for (RouterResourceType routerResourceType: RESOURCE_TYPE_VALUES) {
      routerResourceTypeMap.put(routerResourceType.toString(), routerResourceType);
    }
    return routerResourceTypeMap;
  }

}
