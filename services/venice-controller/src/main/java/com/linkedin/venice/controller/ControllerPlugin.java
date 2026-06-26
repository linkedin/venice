package com.linkedin.venice.controller;

import java.io.Closeable;


/**
 * A pluggable service that runs on the parent Venice controller.
 * Implementations are provided externally (e.g., by downstream projects) and registered one of two ways:
 * <ul>
 *   <li>programmatically, via {@link VeniceControllerContext.Builder#setControllerPluginFactories}; or</li>
 *   <li>by class name, via the {@code controller.plugin.class.names} config, instantiated by reflection
 *       (the class must expose a public constructor taking {@code VeniceParentHelixAdmin},
 *       {@code AuthorizerService}, {@code VeniceControllerMultiClusterConfig}).</li>
 * </ul>
 *
 * <p>The lifecycle is:
 * <ol>
 *   <li>{@link ControllerPluginFactory#create} — called during controller construction</li>
 *   <li>{@link #start()} — called when the controller starts</li>
 *   <li>{@link #close()} — called when the controller stops</li>
 * </ol>
 */
public interface ControllerPlugin extends Closeable {
  /**
   * Called once during controller startup. May start background threads.
   */
  void start();

  /**
   * Returns a human-readable name for logging.
   */
  String getName();
}
