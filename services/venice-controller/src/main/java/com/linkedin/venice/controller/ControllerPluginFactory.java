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
  /**
   * @param admin the parent controller admin
   * @param authorizerService the controller's authorizer, or {@code null} when controller authorization is
   *                           disabled. Implementations that require it must null-check and return {@code null}
   *                           (skip the plugin) rather than dereferencing it.
   * @param config the multi-cluster config, for reading plugin-specific properties
   * @return the plugin to run, or {@code null} to skip creation
   */
  ControllerPlugin create(
      VeniceParentHelixAdmin admin,
      AuthorizerService authorizerService,
      VeniceControllerMultiClusterConfig config);
}
