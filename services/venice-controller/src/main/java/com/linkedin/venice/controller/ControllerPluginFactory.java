package com.linkedin.venice.controller;

import com.linkedin.venice.authorization.AuthorizerService;


/**
 * Factory to create a {@link ControllerPlugin}. Receives all controller context needed to
 * initialize the plugin: the parent admin for store enumeration and leadership checks,
 * the authorizer service, and the multi-cluster config for reading plugin-specific properties.
 *
 * <p>Factories are registered via {@link VeniceControllerContext.Builder#setControllerPluginFactories}
 * and invoked during controller construction. A factory may return {@code null} to indicate
 * the plugin should not be created (e.g., when disabled by config).
 */
@FunctionalInterface
public interface ControllerPluginFactory {
  ControllerPlugin create(
      VeniceParentHelixAdmin admin,
      AuthorizerService authorizerService,
      VeniceControllerMultiClusterConfig config);
}
