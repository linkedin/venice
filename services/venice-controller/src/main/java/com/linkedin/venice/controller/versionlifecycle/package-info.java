/**
 * Store-version lifecycle: add, increment, retire, delete, roll-forward, roll-back, and the
 * topic / Helix resource plumbing that hangs off those transitions. Code is currently spread
 * across {@code VeniceHelixAdmin} and {@code VeniceParentHelixAdmin}; this package is the
 * landing zone for the upcoming extraction.
 */
package com.linkedin.venice.controller.versionlifecycle;
