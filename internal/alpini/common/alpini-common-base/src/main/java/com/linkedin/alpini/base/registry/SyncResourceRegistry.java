package com.linkedin.alpini.base.registry;

/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class SyncResourceRegistry extends ResourceRegistry implements ResourceRegistry.Sync, ShutdownableResource {
  /**
   * Construct a ResourceRegistry class instance. When instances of these {@link ResourceRegistry} are garbage
   * collected when not shut down, they will automatically behave as-if {@link #shutdown()} was invoked and a
   * warning is emitted to the logs.
   */
  public SyncResourceRegistry() {
  }

  /**
   * Construct a ResourceRegistry class instance. Instances constructed with this constructor will not emit any log
   * when it is garbage collected.
   *
   * @param garbageCollectable If true, the {@linkplain ResourceRegistry} will be shut down when no remaining
   *                           references to the {@linkplain ResourceRegistry} remain referenced.
   */
  public SyncResourceRegistry(boolean garbageCollectable) {
    super(garbageCollectable);
  }
}
