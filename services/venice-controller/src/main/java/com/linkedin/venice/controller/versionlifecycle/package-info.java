/**
 * Store-version lifecycle: add, increment, retire, delete, roll-forward, roll-back, and the
 * topic / Helix resource plumbing that hangs off those transitions. The extraction out of
 * {@code VeniceHelixAdmin} and {@code VeniceParentHelixAdmin} is in progress:
 * {@link com.linkedin.venice.controller.versionlifecycle.VersionLifecyclePolicy} (pure-statics
 * decision helpers — capacity guards, version selection, status aggregation, TTL repush flag)
 * has landed; {@code TopicLifecycleManager}, {@code VersionRetirementManager},
 * {@code VersionProvisioner}, and {@code CurrentVersionManager} are planned.
 */
package com.linkedin.venice.controller.versionlifecycle;
