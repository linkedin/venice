package com.linkedin.venice.controller;

import java.io.Closeable;


/**
 * A pluggable service that runs on the parent Venice controller.
 * Implementations are provided externally (e.g., by downstream projects) and registered
 * via {@link VeniceControllerContext.Builder#setControllerPluginFactories}.
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
